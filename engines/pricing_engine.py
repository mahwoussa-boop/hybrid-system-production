"""
engines/pricing_engine.py — محرك التسعير + Transactional Outbox (Sprint 4)
═══════════════════════════════════════════════════════════════════════════════
• يُقيِّم سعرنا مقابل وسط (Median) السوق التنافسي
• لا يُرسِل HTTP — يرمي القرار في outbox_events ليُرسَل لاحقاً
• يحفظ التفاصيل في pricing_decisions للمراجعة والتدقيق
• Idempotent: نفس المنتج في نفس الجلسة لا يُولِّد حدثين (idempotency_key)

قواعد التسعير:
  URGENT_DROP  : سعرنا > وسط السوق بأكثر من 25%   → قرار عاجل
  OVERPRICED   : سعرنا > وسط السوق بـ 10%-25%     → يُعرض للمراجعة
  UNDERPRICED  : سعرنا < وسط السوق بأكثر من 15%   → فرصة رفع السعر
  COMPETITIVE  : سعرنا ضمن ±10% من وسط السوق      → لا إجراء
"""
from __future__ import annotations

import hashlib
import json
import logging
import statistics
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ── استيراد دوال DB من Sprint 1 ─────────────────────────────────────────
try:
    from utils.db_manager import (
        insert_outbox_event,
        get_db_connection,
        DB_PATH as _PERFUME_DB,
    )
    _DB_OK = True
except Exception as _e:
    logger.warning("pricing_engine: تعذّر استيراد db_manager — %s", _e)
    insert_outbox_event = lambda *a, **kw: -1
    get_db_connection   = None
    _PERFUME_DB         = None
    _DB_OK              = False

# ── حدود التسعير (قابلة للتعديل من config.py) ────────────────────────────
try:
    from config import PRICE_TOLERANCE as _PT
    _OVERPRICED_THRESHOLD   = max(float(_PT), 10.0) / 100   # 10% افتراضي
except Exception:
    _OVERPRICED_THRESHOLD   = 0.10

_URGENT_THRESHOLD   = 0.25   # >25% فوق الوسط → عاجل
_UNDERPRICED_THR    = 0.15   # <15% دون الوسط → فرصة رفع


# ══════════════════════════════════════════════════════════════════════════
#  1. حساب توصية التسعير
# ══════════════════════════════════════════════════════════════════════════
def _compute_pricing_recommendation(
    our_price: float,
    competitor_prices: List[float],
) -> Dict[str, Any]:
    """
    يحسب إحصاءات السوق ويُرجع توصية التسعير.
    يُرجع dict:
      recommendation (str), market_median (float), market_min (float),
      market_max (float), competitors_count (int),
      pct_diff (float), suggested_price (float)
    """
    valid = [p for p in (competitor_prices or []) if isinstance(p, (int, float)) and p > 0]
    if not valid:
        return {
            "recommendation":   "NO_DATA",
            "market_median":    0.0,
            "market_min":       0.0,
            "market_max":       0.0,
            "competitors_count": 0,
            "pct_diff":         0.0,
            "suggested_price":  our_price,
        }

    median  = statistics.median(valid)
    pct_diff = (our_price - median) / median if median > 0 else 0.0

    if pct_diff > _URGENT_THRESHOLD:
        recommendation  = "URGENT_DROP"
        # اقتراح: نقل السعر إلى Median + هامش 5%
        suggested_price = round(median * 1.05, 2)
    elif pct_diff > _OVERPRICED_THRESHOLD:
        recommendation  = "OVERPRICED"
        suggested_price = round(median * 1.08, 2)
    elif pct_diff < -_UNDERPRICED_THR:
        recommendation  = "UNDERPRICED"
        suggested_price = round(median * 1.10, 2)
    else:
        recommendation  = "COMPETITIVE"
        suggested_price = our_price

    return {
        "recommendation":    recommendation,
        "market_median":     round(median, 2),
        "market_min":        round(min(valid), 2),
        "market_max":        round(max(valid), 2),
        "competitors_count": len(valid),
        "pct_diff":          round(pct_diff * 100, 2),
        "suggested_price":   suggested_price,
    }


# ══════════════════════════════════════════════════════════════════════════
#  2. حفظ قرار التسعير في pricing_decisions
# ══════════════════════════════════════════════════════════════════════════
def _save_pricing_decision(
    our_product_id:    str,
    our_product_name:  str,
    our_price:         float,
    analysis:          Dict[str, Any],
    outbox_event_id:   int = None,
    db_path:           str = None,
) -> int:
    """
    يُدرج سجلاً في pricing_decisions.
    يُرجع id السجل الجديد أو -1 عند الفشل.
    """
    if not _DB_OK or get_db_connection is None:
        return -1
    try:
        with get_db_connection(db_path) as conn:
            cur = conn.execute(
                """INSERT INTO pricing_decisions
                   (our_product_id, our_product_name, old_price,
                    new_price_suggested, recommendation,
                    competitor_min, competitor_max, competitor_median,
                    competitors_count, outbox_event_id, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    (our_product_id or "")[:100],
                    (our_product_name or "")[:400],
                    float(our_price or 0),
                    float(analysis.get("suggested_price") or our_price),
                    analysis.get("recommendation", "NO_DATA")[:30],
                    float(analysis.get("market_min") or 0),
                    float(analysis.get("market_max") or 0),
                    float(analysis.get("market_median") or 0),
                    int(analysis.get("competitors_count") or 0),
                    outbox_event_id,
                    datetime.now().isoformat(),
                ),
            )
            return cur.lastrowid or -1
    except Exception as exc:
        logger.warning("_save_pricing_decision: فشل حفظ قرار التسعير — %s", exc)
        return -1


# ══════════════════════════════════════════════════════════════════════════
#  3. evaluate_and_store_pricing — الدالة الرئيسية
# ══════════════════════════════════════════════════════════════════════════
def evaluate_and_store_pricing(
    our_product_id:    str,
    our_product_name:  str,
    our_price:         float,
    competitor_prices: List[float],
    competitor_names:  Optional[List[str]] = None,
    db_path:           str = None,
) -> Dict[str, Any]:
    """
    يُقيِّم سعرنا مقابل وسط السوق ويُخزِّن القرار بأمان.

    السلوك الأساسي:
    ─────────────────
    COMPETITIVE → لا حدث Outbox (لا إجراء مطلوب).
    OVERPRICED / UNDERPRICED / URGENT_DROP →
        1. يُنشئ حدث Outbox في outbox_events (ليُرسَل لاحقاً إلى Make.com).
        2. يُخزِّن التفاصيل في pricing_decisions.
        3. يمنع التكرار عبر idempotency_key.

    يُرجع dict:
      recommendation, market_median, pct_diff, suggested_price,
      outbox_event_id (int|None), pricing_decision_id (int|None),
      action_taken (bool)

    لا يُرسِل أي طلب HTTP — الإرسال تتولاه خدمة منفصلة تقرأ outbox_events.
    """
    our_price = float(our_price or 0)

    analysis = _compute_pricing_recommendation(our_price, competitor_prices)
    result: Dict[str, Any] = {
        **analysis,
        "our_product_id":   our_product_id,
        "our_product_name": our_product_name,
        "our_price":        our_price,
        "outbox_event_id":  None,
        "pricing_decision_id": None,
        "action_taken":     False,
    }

    recommendation = analysis.get("recommendation", "NO_DATA")
    logger.info(
        "pricing_eval: «%.50s» | سعرنا=%s | وسط=%s | %s (%.1f%%)",
        our_product_name, our_price,
        analysis.get("market_median"), recommendation,
        analysis.get("pct_diff", 0),
    )

    # COMPETITIVE أو NO_DATA → لا إجراء
    if recommendation in ("COMPETITIVE", "NO_DATA"):
        return result

    # ── بناء idempotency_key منع التكرار ─────────────────────────────────
    idem_raw  = f"pricing:{our_product_id}:{recommendation}:{datetime.now().strftime('%Y-%m-%d')}"
    idem_key  = hashlib.sha256(idem_raw.encode()).hexdigest()[:40]

    # ── بناء payload للـ Outbox ───────────────────────────────────────────
    payload = {
        "product_id":      our_product_id,
        "product_name":    our_product_name,
        "our_price":       our_price,
        "suggested_price": analysis.get("suggested_price"),
        "recommendation":  recommendation,
        "market_median":   analysis.get("market_median"),
        "market_min":      analysis.get("market_min"),
        "market_max":      analysis.get("market_max"),
        "pct_diff":        analysis.get("pct_diff"),
        "competitors_count": analysis.get("competitors_count"),
        "competitor_names": (competitor_names or [])[:10],
        "generated_at":    datetime.now().isoformat(),
    }

    # ── إدراج في outbox_events (بدون إرسال HTTP) ─────────────────────────
    outbox_id: int = -1
    try:
        outbox_id = insert_outbox_event(
            event_type=f"PRICE_{recommendation}",
            payload_json=json.dumps(payload, ensure_ascii=False, default=str),
            product_id=str(our_product_id or ""),
            product_name=str(our_product_name or ""),
            idempotency_key=idem_key,
            db_path=db_path,
        )
        result["outbox_event_id"] = outbox_id if outbox_id and outbox_id > 0 else None
        if outbox_id and outbox_id > 0:
            logger.info(
                "pricing_engine → outbox_events #%d: %s «%.50s»",
                outbox_id, recommendation, our_product_name,
            )
    except Exception as exc:
        logger.warning("evaluate_and_store_pricing: فشل insert_outbox_event — %s", exc)

    # ── إدراج في pricing_decisions للتدقيق ───────────────────────────────
    pd_id = _save_pricing_decision(
        our_product_id, our_product_name, our_price,
        analysis, outbox_event_id=outbox_id if outbox_id and outbox_id > 0 else None,
        db_path=db_path,
    )
    result["pricing_decision_id"] = pd_id if pd_id and pd_id > 0 else None
    result["action_taken"] = True

    return result
