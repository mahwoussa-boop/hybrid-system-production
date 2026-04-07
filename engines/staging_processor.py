"""
engines/staging_processor.py — معالج الدفعات غير المتزامن (Sprint 4)
═══════════════════════════════════════════════════════════════════════
يربط ثلاث طبقات:
  1. raw_scrape_staging  ← صفوف مكشوطة بانتظار المعالجة
  2. smart_match_product ← Cache-first matching (engine.py)
  3. evaluate_and_store_pricing ← pricing_engine → outbox_events

قواعد صارمة:
  • لا يُعدِّل run_full_analysis أو أي دالة قديمة في engine.py
  • بحد أقصى 20 منتجاً لكل دورة (BATCH_SIZE)
  • await asyncio.sleep(1) إجباري بين كل استدعاء لـ smart_match_product
    (يحمي من Rate-Limit وخنق Event Loop)
  • فشل أي منتج فردي لا يُوقف الدورة كاملة (Graceful Degradation)
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

BATCH_SIZE = 20    # الحد الأقصى لكل دورة — تغيير هنا فقط

# ── استيرادات مع Graceful Degradation ────────────────────────────────────
try:
    from utils.db_manager import (
        get_db_connection,
        mark_staging_processed,
        DB_PATH as _PERFUME_DB,
    )
    _DB_OK = True
except Exception as _e:
    logger.warning("staging_processor: db_manager غير متاح — %s", _e)
    get_db_connection    = None
    mark_staging_processed = lambda *a, **kw: None
    _PERFUME_DB          = None
    _DB_OK               = False

try:
    from engines.engine import smart_match_product
    _ENGINE_OK = True
except Exception as _e:
    logger.warning("staging_processor: engine غير متاح — %s", _e)
    smart_match_product = None
    _ENGINE_OK          = False

try:
    from engines.pricing_engine import evaluate_and_store_pricing
    _PRICING_OK = True
except Exception as _e:
    logger.warning("staging_processor: pricing_engine غير متاح — %s", _e)
    evaluate_and_store_pricing = None
    _PRICING_OK                = False


# ══════════════════════════════════════════════════════════════════════════
#  1. قراءة الدفعة من raw_scrape_staging
# ══════════════════════════════════════════════════════════════════════════
def _read_pending_staging_batch(
    batch_size: int = BATCH_SIZE,
    db_path:    str = None,
) -> List[Dict[str, Any]]:
    """
    يقرأ بحد أقصى batch_size سجلاً بحالة 'pending' من raw_scrape_staging
    مرتبةً من الأقدم للأحدث (FIFO).
    """
    if not _DB_OK or get_db_connection is None:
        return []
    try:
        with get_db_connection(db_path) as conn:
            rows = conn.execute(
                """SELECT id, competitor, url, name, price, image_url,
                          brand, sku, raw_json, scraped_at
                   FROM raw_scrape_staging
                   WHERE status = 'pending'
                   ORDER BY id ASC
                   LIMIT ?""",
                (int(batch_size),),
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception as exc:
        logger.warning("_read_pending_staging_batch: فشل القراءة — %s", exc)
        return []


# ══════════════════════════════════════════════════════════════════════════
#  2. استخراج أسعار المنافسين من raw_json
# ══════════════════════════════════════════════════════════════════════════
def _extract_competitor_prices(record: Dict[str, Any]) -> List[float]:
    """
    يحاول استخراج قائمة أسعار منافسين من raw_json أو يُرجع سعراً واحداً.
    raw_json قد يحتوي: {"competitor_prices": [...]} أو {"price": X}.
    """
    prices = []
    try:
        raw = record.get("raw_json") or ""
        if raw:
            data = json.loads(raw) if isinstance(raw, str) else raw
            cp = data.get("competitor_prices") or data.get("prices") or []
            prices = [float(p) for p in cp if p]
    except Exception:
        pass

    # fallback: السعر المباشر للسجل
    try:
        p = float(record.get("price") or 0)
        if p > 0 and p not in prices:
            prices.append(p)
    except Exception:
        pass

    return prices


# ══════════════════════════════════════════════════════════════════════════
#  3. معالجة سجل واحد من staging
# ══════════════════════════════════════════════════════════════════════════
async def _process_staging_record(
    record:  Dict[str, Any],
    db_path: str = None,
) -> Dict[str, Any]:
    """
    يُنفِّذ دورة كاملة لسجل staging واحد:
    match → pricing evaluation → mark processed.
    يُرجع dict بنتيجة المعالجة.
    """
    staging_id   = record.get("id")
    product_name = (record.get("name") or "").strip()
    competitor   = record.get("competitor", "")

    outcome: Dict[str, Any] = {
        "staging_id":     staging_id,
        "product_name":   product_name,
        "matched":        False,
        "recommendation": "SKIPPED",
        "error":          None,
    }

    if not product_name:
        outcome["error"] = "اسم المنتج فارغ"
        mark_staging_processed(staging_id, db_path=db_path)
        return outcome

    if not _ENGINE_OK or smart_match_product is None:
        outcome["error"] = "engine غير محمَّل"
        return outcome

    # ── المطابقة — يُشغِّل smart_match_product (Cache-first) ─────────────
    try:
        comp_price = float(record.get("price") or 0)
        match_result = await asyncio.to_thread(
            smart_match_product,
            product_name,
            None,       # candidates: None في سياق Staging (Cache-Only لأول مرة)
            comp_price,
            db_path,
        )
        outcome["matched"]      = not match_result.get("no_match", True)
        outcome["from_cache"]   = match_result.get("from_cache", False)
        outcome["match_method"] = match_result.get("match_method", "unknown")
        outcome["confidence"]   = match_result.get("confidence_score", 0)
    except Exception as exc:
        outcome["error"] = f"match_error: {str(exc)[:200]}"
        logger.warning("staging record #%s match فشل: %s", staging_id, exc)
        mark_staging_processed(staging_id, db_path=db_path)
        return outcome

    # ── التسعير — فقط عند وجود تطابق وتسعير جاهز ────────────────────────
    if outcome["matched"] and _PRICING_OK and evaluate_and_store_pricing is not None:
        our_prod_id   = match_result.get("our_product_id", "")
        our_prod_name = match_result.get("our_product_name", "")
        our_price     = float(record.get("price") or 0)
        comp_prices   = _extract_competitor_prices(record)

        if our_price > 0 and comp_prices:
            try:
                pricing = await asyncio.to_thread(
                    evaluate_and_store_pricing,
                    our_prod_id,
                    our_prod_name,
                    our_price,
                    comp_prices,
                    [competitor],
                    db_path,
                )
                outcome["recommendation"]  = pricing.get("recommendation", "UNKNOWN")
                outcome["outbox_event_id"] = pricing.get("outbox_event_id")
                outcome["action_taken"]    = pricing.get("action_taken", False)
            except Exception as exc:
                logger.warning("staging #%s pricing فشل: %s", staging_id, exc)
                outcome["error"] = f"pricing_error: {str(exc)[:200]}"

    # ── تحديث الحالة في DB ────────────────────────────────────────────────
    mark_staging_processed(staging_id, db_path=db_path)
    return outcome


# ══════════════════════════════════════════════════════════════════════════
#  4. process_staging_batch_async — الدالة الرئيسية (المطلوبة في Sprint 4)
# ══════════════════════════════════════════════════════════════════════════
async def process_staging_batch_async(
    batch_size: int = BATCH_SIZE,
    db_path:    str = None,
) -> Dict[str, Any]:
    """
    يعالج دفعة من raw_scrape_staging بشكل غير متزامن.
    ═══════════════════════════════════════════════════
    الخطوات:
      1. يقرأ بحد أقصى batch_size سجلاً 'pending' (FIFO).
      2. لكل سجل:
         أ. يستدعي smart_match_product (Cache-first).
         ب. عند التطابق: يستدعي evaluate_and_store_pricing.
         ج. يُحدِّث الحالة إلى 'processed'.
      3. await asyncio.sleep(1) إجباري بين كل سجلين — يحمي من
         Rate-Limit الـ Gemini API وخنق asyncio Event Loop.
      4. فشل سجل واحد لا يُوقف الدورة.

    يُرجع dict إحصائي:
      processed, matched, pricing_actions, errors, duration_sec
    """
    started_at = datetime.now()
    stats: Dict[str, Any] = {
        "batch_size":      batch_size,
        "read":            0,
        "processed":       0,
        "matched":         0,
        "cache_hits":      0,
        "pricing_actions": 0,
        "errors":          0,
        "started_at":      started_at.isoformat(),
        "finished_at":     None,
        "duration_sec":    None,
    }

    db_path = db_path or _PERFUME_DB

    if not _DB_OK:
        logger.warning("process_staging_batch_async: قاعدة البيانات غير متاحة")
        stats["finished_at"]  = datetime.now().isoformat()
        stats["duration_sec"] = 0
        return stats

    # ── قراءة الدفعة ──────────────────────────────────────────────────────
    records = _read_pending_staging_batch(batch_size=batch_size, db_path=db_path)
    stats["read"] = len(records)

    if not records:
        logger.info("process_staging_batch_async: لا سجلات pending في staging")
        stats["finished_at"]  = datetime.now().isoformat()
        stats["duration_sec"] = 0
        return stats

    logger.info(
        "process_staging_batch_async: بدأ معالجة %d سجل (batch_size=%d)",
        len(records), batch_size,
    )

    # ── حلقة المعالجة — sleep(1) إجباري بين كل سجلين ────────────────────
    for idx, record in enumerate(records):
        try:
            outcome = await _process_staging_record(record, db_path=db_path)
            stats["processed"] += 1

            if outcome.get("matched"):
                stats["matched"] += 1
            if outcome.get("from_cache"):
                stats["cache_hits"] += 1
            if outcome.get("action_taken"):
                stats["pricing_actions"] += 1
            if outcome.get("error"):
                stats["errors"] += 1
                logger.debug(
                    "staging #%s خطأ: %s",
                    record.get("id"), outcome["error"],
                )

        except Exception as exc:
            stats["errors"] += 1
            logger.warning(
                "process_staging_batch_async: سجل #%s انهار — %s",
                record.get("id"), exc,
            )

        # ── sleep إجباري بين الاستدعاءات — يحمي من Rate-Limit ──────────
        # حتى لو كان آخر سجل: يُضاف للاتساق ويُتيح Event Loop للتنفس
        if idx < len(records) - 1:
            await asyncio.sleep(1)

    finished_at = datetime.now()
    stats["finished_at"]  = finished_at.isoformat()
    stats["duration_sec"] = round((finished_at - started_at).total_seconds(), 2)

    logger.info(
        "process_staging_batch_async: اكتمل — %d معالج | %d مطابق | "
        "%d cache | %d قرار تسعير | %d خطأ | %.1f ثانية",
        stats["processed"], stats["matched"], stats["cache_hits"],
        stats["pricing_actions"], stats["errors"], stats["duration_sec"],
    )
    return stats
