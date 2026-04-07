#!/usr/bin/env python3
"""
scripts/clean_bad_matches.py — تنظيف المطابقات الفاسدة من كاش match_cache_v21.db
═══════════════════════════════════════════════════════════════════════════════════
يمسح كل إدخال حُفظ بتناقض واضح في الأحجام أو التركيزات.
يستخدم نفس دالة verify_perfume_match من engines/ai_engine.py.

الاستخدام:
    python scripts/clean_bad_matches.py [--dry-run] [--verbose]

الخيارات:
    --dry-run   أظهر ما سيُحذف فقط دون تنفيذ الحذف الفعلي
    --verbose   اطبع تفاصيل كل إدخال محذوف
"""
import argparse
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_log = logging.getLogger("clean_bad_matches")

# ── مسار قاعدة البيانات ──────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))   # حتى يعمل import engines.*

try:
    from utils.data_paths import get_data_db_path
    _DB_PATH = get_data_db_path("match_cache_v21.db")
except Exception:
    _DB_PATH = str(_REPO_ROOT / "data" / "match_cache_v21.db")

# ── استيراد دالة الفحص ───────────────────────────────────────────────────────
try:
    from engines.ai_engine import verify_perfume_match
    _log.info("تم استيراد verify_perfume_match بنجاح")
except Exception as _imp_err:
    _log.error("فشل استيراد verify_perfume_match: %s", _imp_err)
    sys.exit(1)


def _load_all_rows(conn: sqlite3.Connection) -> list:
    """يُرجع كل صفوف الكاش [(hash, value_json, ts), ...]."""
    try:
        return conn.execute("SELECT h, v, ts FROM cache").fetchall()
    except sqlite3.OperationalError as e:
        _log.error("خطأ في قراءة الجدول: %s", e)
        return []


def _parse_cached_value(raw_json: str) -> dict | None:
    """يحلل قيمة JSON المُخزَّنة. يُرجع None إذا فشل."""
    try:
        return json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return None


def _extract_names_from_cache(val: dict) -> tuple[str, str]:
    """
    يستخرج اسم منتجنا + اسم المنافس من قيمة الكاش.
    هيكل الكاش يمكن أن يكون:
      - [[index], [reason]]   (نتيجة _ai_batch)
      - {"our": "...", "comp": "..."}
      - {"match": true/false, "our_name": "...", "comp_name": "..."}
    """
    if not isinstance(val, dict):
        # قائمة أرقام (cache قديم من _ai_batch) — لا يمكن مقارنة الأسماء
        return "", ""

    # محاولة استخراج الأسماء من حقول مختلفة
    n1 = (
        val.get("our_name") or val.get("our") or
        val.get("p1") or val.get("product") or ""
    )
    n2 = (
        val.get("comp_name") or val.get("comp") or
        val.get("p2") or val.get("competitor") or ""
    )
    return str(n1).strip(), str(n2).strip()


def _row_is_bad(val: dict) -> tuple[bool, str]:
    """
    يفحص إدخاماً واحداً من الكاش.
    يُرجع (is_bad, reason).
    """
    n1, n2 = _extract_names_from_cache(val)

    # إذا كان match=false المُخزَّن → ليس خطأ (الكاش صحيح)
    if isinstance(val, dict) and val.get("match") is False:
        return False, ""

    # إذا لم نستطع استخراج الأسماء → تخطَّ
    if not n1 or not n2:
        return False, ""

    result = verify_perfume_match(n1, n2)
    if not result["ok"]:
        return True, result["reason"]
    return False, ""


def main() -> None:
    parser = argparse.ArgumentParser(
        description="تنظيف مطابقات عطور فاسدة من match_cache_v21.db"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="أظهر الإدخالات الفاسدة فقط دون حذفها",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="اطبع تفاصيل كل إدخال يُفحص",
    )
    parser.add_argument(
        "--db", default=_DB_PATH,
        help=f"مسار قاعدة البيانات (افتراضي: {_DB_PATH})",
    )
    args = parser.parse_args()

    db_path = args.db
    if not os.path.exists(db_path):
        _log.error("ملف قاعدة البيانات غير موجود: %s", db_path)
        sys.exit(1)

    _log.info("فتح قاعدة البيانات: %s", db_path)
    conn = sqlite3.connect(db_path, check_same_thread=False)

    rows = _load_all_rows(conn)
    _log.info("إجمالي الإدخالات في الكاش: %d", len(rows))

    bad_hashes: list[str] = []
    stats = {"total": len(rows), "checked": 0, "bad": 0, "skipped": 0}

    for h, raw_v, ts in rows:
        val = _parse_cached_value(raw_v)
        if val is None:
            stats["skipped"] += 1
            continue

        stats["checked"] += 1
        is_bad, reason = _row_is_bad(val)

        if is_bad:
            stats["bad"] += 1
            bad_hashes.append(h)
            n1, n2 = _extract_names_from_cache(val) if isinstance(val, dict) else ("?", "?")
            if args.verbose or args.dry_run:
                _log.info(
                    "❌ فاسد: «%.50s» ↔ «%.50s»\n   السبب: %s\n   Hash: %s",
                    n1, n2, reason, h,
                )
        else:
            if args.verbose:
                n1, n2 = _extract_names_from_cache(val) if isinstance(val, dict) else ("?", "?")
                _log.debug("✅ سليم: %.50s ↔ %.50s", n1, n2)

    _log.info(
        "النتيجة: %d فاسد من أصل %d (فُحص: %d | تخطَّى: %d)",
        stats["bad"], stats["total"], stats["checked"], stats["skipped"],
    )

    if not bad_hashes:
        _log.info("✅ لا توجد مطابقات فاسدة — الكاش نظيف.")
        conn.close()
        return

    if args.dry_run:
        _log.info("--dry-run: لم يُحذف أي شيء. لتنفيذ الحذف أعد التشغيل بدون --dry-run")
        conn.close()
        return

    # ── الحذف الفعلي ──────────────────────────────────────────────────────────
    try:
        # حذف على دفعات لتجنب حدّ 999 معامل في SQLite
        BATCH = 500
        deleted = 0
        for start in range(0, len(bad_hashes), BATCH):
            chunk = bad_hashes[start: start + BATCH]
            placeholders = ",".join("?" * len(chunk))
            conn.execute(f"DELETE FROM cache WHERE h IN ({placeholders})", chunk)
            deleted += len(chunk)
        conn.commit()
        _log.info("🗑️  تم حذف %d إدخال فاسد من الكاش", deleted)
    except sqlite3.Error as e:
        _log.error("خطأ أثناء الحذف: %s", e)
        conn.rollback()
    finally:
        conn.close()

    _log.info("✅ اكتمل التنظيف — أعد تشغيل التحليل للحصول على نتائج صحيحة.")


if __name__ == "__main__":
    main()
