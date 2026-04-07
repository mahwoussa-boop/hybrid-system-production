"""
engines/automation.py v26.0 — محرك الأتمتة الذكي الكامل
════════════════════════════════════════════════════════
✅ قواعد تسعير قابلة للتخصيص (Rules Engine)
✅ اتخاذ قرارات تلقائية بناءً على نسبة التطابق والفرق السعري
✅ أتمتة الإرسال إلى Make.com/سلة
✅ سجل كامل لكل قرار آلي
✅ جدولة عمليات بحث دورية
✅ حماية ضد القرارات الخاطئة (حدود أمان)
"""
import json
import time
import threading
import sqlite3
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd

try:
    from config import (AUTOMATION_RULES_DEFAULT, AUTO_DECISION_CONFIDENCE,
                        AUTO_PUSH_TO_MAKE, AUTO_SEARCH_INTERVAL_MINUTES)
except ImportError:
    AUTOMATION_RULES_DEFAULT = []
    AUTO_DECISION_CONFIDENCE = 92
    AUTO_PUSH_TO_MAKE = False
    AUTO_SEARCH_INTERVAL_MINUTES = 360

# مسار DB الموحّد — دائماً من db_manager
from utils.db_manager import DB_PATH


# ═══════════════════════════════════════════════════════
#  1. محرك القواعد (Rules Engine)
# ═══════════════════════════════════════════════════════
class PricingRule:
    """قاعدة تسعير واحدة قابلة للتقييم"""

    def __init__(self, rule_dict: dict):
        self.name = rule_dict.get("name", "قاعدة بدون اسم")
        self.enabled = rule_dict.get("enabled", True)
        self.action = rule_dict.get("action", "keep")
        self.min_match_score = rule_dict.get("min_match_score", 90)
        self.params = rule_dict

    def evaluate(self, our_price: float, comp_price: float,
                 match_score: float, cost_price: float = 0) -> Optional[Dict]:
        """تقييم القاعدة — يُعيد dict بالقرار أو None"""
        if not self.enabled or match_score < self.min_match_score:
            return None
        if our_price <= 0 or comp_price <= 0:
            return None

        diff = our_price - comp_price

        if self.action == "undercut":
            min_diff = self.params.get("min_diff", 10)
            undercut = self.params.get("undercut_amount", 1)
            max_loss_pct = self.params.get("max_loss_pct", 15)
            if diff > min_diff:
                new_price = comp_price - undercut
                if cost_price > 0:
                    # حماية مستندة إلى سعر التكلفة الفعلي
                    min_allowed = cost_price * (1 - max_loss_pct / 100)
                    new_price = max(new_price, round(min_allowed, 2))
                elif our_price > 0:
                    # حماية افتراضية عند غياب سعر التكلفة:
                    # لا تخفض أكثر من max_loss_pct% من سعرنا الحالي
                    min_allowed = our_price * (1 - max_loss_pct / 100)
                    new_price = max(new_price, round(min_allowed, 2))
                if new_price < 1:
                    return None
                return {
                    "rule": self.name, "action": "lower_price",
                    "old_price": our_price, "new_price": round(new_price, 2),
                    "reason": f"سعرنا أعلى بـ {diff:.0f} ر.س — خفض ليصبح أقل من المنافس بـ {undercut} ر.س",
                }

        elif self.action == "raise_to_match":
            min_diff = self.params.get("min_diff", 15)
            margin = self.params.get("margin_below", 5)
            if diff < -min_diff:
                new_price = comp_price - margin
                if new_price <= our_price:
                    return None
                return {
                    "rule": self.name, "action": "raise_price",
                    "old_price": our_price, "new_price": round(new_price, 2),
                    "reason": f"فرصة ربح: سعرنا أقل بـ {abs(diff):.0f} ر.س — رفع ليصبح أقل من المنافس بـ {margin} ر.س",
                }

        elif self.action == "keep":
            threshold = self.params.get("threshold", 10)
            if abs(diff) <= threshold:
                return {
                    "rule": self.name, "action": "keep_price",
                    "old_price": our_price, "new_price": our_price,
                    "reason": f"السعر تنافسي — الفرق {diff:+.0f} ر.س ضمن الهامش المقبول",
                }

        return None


class AutomationEngine:
    """محرك الأتمتة الرئيسي"""

    def __init__(self, rules: List[dict] = None):
        self.rules = [PricingRule(r) for r in (rules or AUTOMATION_RULES_DEFAULT)]
        self.decisions_log: List[dict] = []
        self._lock = threading.Lock()

    def evaluate_product(self, product_data: dict) -> Optional[Dict]:
        """تقييم منتج واحد ضد كل القواعد"""
        our_price = float(product_data.get("our_price", 0))
        comp_price = float(product_data.get("comp_price", 0))
        match_score = float(product_data.get("match_score", 0))
        cost_price = float(product_data.get("cost_price", 0))

        for rule in self.rules:
            decision = rule.evaluate(our_price, comp_price, match_score, cost_price)
            if decision:
                decision.update({
                    "product_name": product_data.get("name", ""),
                    "product_id": product_data.get("product_id", ""),
                    "competitor": product_data.get("competitor", ""),
                    "comp_price": comp_price,
                    "match_score": match_score,
                    "timestamp": datetime.now().isoformat(),
                })
                with self._lock:
                    self.decisions_log.append(decision)
                return decision
        return None

    def evaluate_batch(self, products_df: pd.DataFrame) -> List[Dict]:
        """تقييم دفعة من المنتجات"""
        decisions = []
        for _, row in products_df.iterrows():
            d = self.evaluate_product({
                "name": str(row.get("المنتج", "")),
                "our_price": float(row.get("السعر", 0) or 0),
                "comp_price": float(row.get("سعر_المنافس", 0) or 0),
                "match_score": float(row.get("نسبة_التطابق", 0) or 0),
                "product_id": str(row.get("معرف_المنتج", "")),
                "competitor": str(row.get("المنافس", "")),
            })
            if d:
                decisions.append(d)
        return decisions

    def get_summary(self) -> Dict:
        """ملخص إحصائي"""
        with self._lock:
            log = list(self.decisions_log)
        if not log:
            return {"total": 0, "lower": 0, "raise": 0, "keep": 0,
                    "savings": 0, "gains": 0, "net_impact": 0}
        lower_c = sum(1 for d in log if d["action"] == "lower_price")
        raise_c = sum(1 for d in log if d["action"] == "raise_price")
        keep_c = sum(1 for d in log if d["action"] == "keep_price")
        savings = sum(d["old_price"] - d["new_price"] for d in log if d["action"] == "lower_price")
        gains = sum(d["new_price"] - d["old_price"] for d in log if d["action"] == "raise_price")
        return {
            "total": len(log), "lower": lower_c, "raise": raise_c, "keep": keep_c,
            "savings": round(savings, 2), "gains": round(gains, 2),
            "net_impact": round(gains - savings, 2),
        }

    def clear_log(self):
        with self._lock:
            self.decisions_log.clear()


# ═══════════════════════════════════════════════════════
#  2. إرسال تلقائي إلى Make.com
# ═══════════════════════════════════════════════════════
def auto_push_decisions(decisions: List[Dict]) -> Dict:
    """إرسال القرارات المؤهلة إلى Make.com"""
    try:
        from utils.make_helper import send_batch_smart
    except ImportError:
        return {"success": False, "sent": 0, "message": "make_helper غير متاح"}

    eligible = [
        d for d in decisions
        if d.get("match_score", 0) >= AUTO_DECISION_CONFIDENCE
        and d.get("action") in ("lower_price", "raise_price")
        and d.get("product_id")
    ]
    if not eligible:
        return {"success": True, "sent": 0, "message": "لا توجد قرارات مؤهلة للإرسال"}

    products = [{
        "product_id": d["product_id"], "name": d["product_name"],
        "price": d["new_price"], "old_price": d["old_price"],
        "section": "auto_" + d["action"], "reason": d["reason"],
        "confidence": d["match_score"], "competitor": d.get("competitor", ""),
    } for d in eligible]

    try:
        result = send_batch_smart(products, "auto_update")
        # تسجيل الإرسال
        for d in eligible:
            log_automation_decision(d, pushed=True)
        return {"success": True, "sent": len(products), "result": result,
                "message": f"تم إرسال {len(products)} تحديث تلقائي"}
    except Exception as e:
        return {"success": False, "sent": 0, "message": f"فشل: {str(e)[:200]}"}


def auto_process_review_items(review_df: pd.DataFrame) -> pd.DataFrame:
    """معالجة تلقائية لقسم المراجعة بالتحقق المزدوج من AI"""
    try:
        from engines.ai_engine import verify_match
    except ImportError:
        return pd.DataFrame()

    confirmed = []
    for _, row in review_df.iterrows():
        our_name = str(row.get("المنتج", ""))
        comp_name = str(row.get("منتج_المنافس", ""))
        if not our_name or not comp_name:
            continue
        try:
            v = verify_match(our_name, comp_name,
                             float(row.get("السعر", 0) or 0),
                             float(row.get("سعر_المنافس", 0) or 0))
            if v.get("match") and float(v.get("confidence", 0)) >= AUTO_DECISION_CONFIDENCE:
                rd = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
                cs = v.get("correct_section", "")
                if cs:
                    rd["القرار"] = cs
                rd["_auto_verified"] = True
                rd["_verification_confidence"] = v.get("confidence", 0)
                confirmed.append(rd)
        except Exception:
            continue
    return pd.DataFrame(confirmed) if confirmed else pd.DataFrame()


# ═══════════════════════════════════════════════════════
#  3. جدولة البحث الدوري
# ═══════════════════════════════════════════════════════
class ScheduledSearchManager:
    """مدير البحث الدوري عن أسعار المنافسين"""

    def __init__(self, interval_minutes: int = None):
        self.interval = timedelta(minutes=interval_minutes or AUTO_SEARCH_INTERVAL_MINUTES)
        self.last_run: Optional[datetime] = None
        self.last_results: List[Dict] = []
        self.is_running = False
        self._lock = threading.Lock()

    def should_run(self) -> bool:
        if self.last_run is None:
            return True
        return datetime.now() - self.last_run >= self.interval

    def time_until_next(self) -> str:
        if self.last_run is None:
            return "لم يتم التشغيل بعد"
        remaining = (self.last_run + self.interval) - datetime.now()
        if remaining.total_seconds() <= 0:
            return "حان وقت التشغيل"
        h = int(remaining.total_seconds() // 3600)
        m = int((remaining.total_seconds() % 3600) // 60)
        return f"{h} ساعة و {m} دقيقة"

    def run_scan(self, products_df: pd.DataFrame, top_n: int = 20) -> List[Dict]:
        """مسح السوق لأهم المنتجات"""
        try:
            from engines.ai_engine import search_market_price
        except ImportError:
            return []

        with self._lock:
            if self.is_running:
                return []
            self.is_running = True

        try:
            results = []
            if "الفرق" in products_df.columns:
                sorted_df = products_df.sort_values("الفرق", key=abs, ascending=False).head(top_n)
            else:
                sorted_df = products_df.head(top_n)

            for _, row in sorted_df.iterrows():
                name = str(row.get("المنتج", ""))
                price = float(row.get("السعر", 0) or 0)
                if not name or price <= 0:
                    continue
                try:
                    market = search_market_price(name, price)
                    if market.get("success"):
                        results.append({
                            "product": name, "our_price": price,
                            "market_data": market,
                            "timestamp": datetime.now().isoformat(),
                        })
                except Exception:
                    continue
                time.sleep(1)

            with self._lock:
                self.last_run = datetime.now()
                self.last_results = results
                self.is_running = False
            return results
        except Exception:
            with self._lock:
                self.is_running = False
            return []


# ═══════════════════════════════════════════════════════
#  4. تسجيل في قاعدة البيانات
# ═══════════════════════════════════════════════════════
def _ensure_automation_table(db=None):
    """إنشاء جدول الأتمتة إذا لم يكن موجوداً"""
    path = db or DB_PATH
    try:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS automation_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT DEFAULT (datetime('now','localtime')),
                product_name TEXT,
                product_id TEXT,
                rule_name TEXT,
                action TEXT,
                old_price REAL,
                new_price REAL,
                comp_price REAL,
                competitor TEXT,
                match_score REAL,
                reason TEXT,
                pushed_to_make INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
    except Exception:
        pass


def log_automation_decision(decision: dict, pushed: bool = False, db=None):
    """تسجيل قرار أتمتة"""
    path = db or DB_PATH
    _ensure_automation_table(path)
    try:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute(
            """INSERT INTO automation_log
               (product_name, product_id, rule_name, action, old_price,
                new_price, comp_price, competitor, match_score, reason, pushed_to_make)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (decision.get("product_name", ""), decision.get("product_id", ""),
             decision.get("rule", ""), decision.get("action", ""),
             decision.get("old_price", 0), decision.get("new_price", 0),
             decision.get("comp_price", 0), decision.get("competitor", ""),
             decision.get("match_score", 0), decision.get("reason", ""),
             1 if pushed else 0)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_automation_log(limit: int = 50, db=None) -> List[Dict]:
    """استرجاع سجل الأتمتة"""
    path = db or DB_PATH
    _ensure_automation_table(path)
    try:
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM automation_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_automation_stats(days: int = 7, db=None) -> Dict:
    """إحصائيات الأتمتة لآخر X يوم"""
    path = db or DB_PATH
    _ensure_automation_table(path)
    try:
        conn = sqlite3.connect(path, check_same_thread=False)
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        total = conn.execute(
            "SELECT COUNT(*) FROM automation_log WHERE timestamp>=?", (since,)
        ).fetchone()[0]
        lower = conn.execute(
            "SELECT COUNT(*) FROM automation_log WHERE timestamp>=? AND action='lower_price'",
            (since,)
        ).fetchone()[0]
        raised = conn.execute(
            "SELECT COUNT(*) FROM automation_log WHERE timestamp>=? AND action='raise_price'",
            (since,)
        ).fetchone()[0]
        pushed = conn.execute(
            "SELECT COUNT(*) FROM automation_log WHERE timestamp>=? AND pushed_to_make=1",
            (since,)
        ).fetchone()[0]
        conn.close()
        return {"total": total, "lower": lower, "raise": raised,
                "keep": total - lower - raised, "pushed": pushed}
    except Exception:
        return {"total": 0, "lower": 0, "raise": 0, "keep": 0, "pushed": 0}
