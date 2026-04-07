"""
utils/outbox_dispatcher.py — مُفرِّغ الـ Outbox غير المتزامن (Final Phase)
═══════════════════════════════════════════════════════════════════════════════
• يقرأ أحداث outbox_events (pending + failed < 3 محاولات)
• يُرسِل كل حدث إلى Make.com Webhook عبر aiohttp (لا يُجمِّد Event Loop)
• 2xx  → mark_outbox_sent(event_id)
• غير 2xx أو Timeout → mark_outbox_failed(event_id) [يُلغى بعد 3 مرات]
• await asyncio.sleep(0.5) إجباري بين كل إرسالين (Rate-Limit Make.com)

Webhook URLs مأخوذة من متغيرات البيئة:
  WEBHOOK_UPDATE_PRICES  ← أحداث التسعير (PRICE_*)
  WEBHOOK_NEW_PRODUCTS   ← أحداث المنتجات الجديدة (NEW_*)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

import aiohttp
from aiohttp import ClientTimeout

from utils.db_manager import (
    get_pending_outbox_events,
    mark_outbox_sent,
    mark_outbox_failed,
    DB_PATH as PERFUME_DB,
)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  إعدادات ثابتة
# ══════════════════════════════════════════════════════════════════════════
_WEBHOOK_PRICES    = os.environ.get("WEBHOOK_UPDATE_PRICES",  "").strip()
_WEBHOOK_NEW_PRODS = os.environ.get("WEBHOOK_NEW_PRODUCTS",   "").strip()

_HTTP_TIMEOUT = ClientTimeout(total=20, connect=8)
_DISPATCH_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent":   "Mahwous-Outbox/4.0",
}
_RATE_LIMIT_SLEEP = 0.5   # ثانية — Rate-Limit Make.com (إجباري)


# ══════════════════════════════════════════════════════════════════════════
#  1. اختيار Webhook URL حسب نوع الحدث
# ══════════════════════════════════════════════════════════════════════════
def _resolve_webhook_url(event_type: str) -> str:
    """
    يُحدِّد Webhook URL المناسب من متغيرات البيئة بناءً على نوع الحدث.
    أحداث التسعير (PRICE_*) → WEBHOOK_UPDATE_PRICES
    أحداث المنتجات الجديدة (NEW_*)  → WEBHOOK_NEW_PRODUCTS
    أي نوع آخر → WEBHOOK_UPDATE_PRICES كـ fallback.
    """
    et = (event_type or "").upper()
    if et.startswith("NEW_") or et.startswith("MISSING"):
        return _WEBHOOK_NEW_PRODS
    return _WEBHOOK_PRICES


# ══════════════════════════════════════════════════════════════════════════
#  2. إرسال حدث واحد
# ══════════════════════════════════════════════════════════════════════════
async def _send_event(
    session:  aiohttp.ClientSession,
    event:    Dict[str, Any],
    db_path:  str = None,
) -> Dict[str, Any]:
    """
    يُرسِل حدثاً واحداً من outbox_events إلى Make.com Webhook.

    النتيجة:
      success=True  → mark_outbox_sent
      success=False → mark_outbox_failed (يزيد attempts؛ بعد 3 → 'dead')
    يُرجع dict بنتيجة الإرسال لأغراض الإحصاء.
    """
    event_id    = event.get("id")
    event_type  = event.get("event_type", "UNKNOWN")
    payload_str = event.get("payload_json") or "{}"
    attempts    = int(event.get("attempts") or 0)

    result: Dict[str, Any] = {
        "event_id":    event_id,
        "event_type":  event_type,
        "attempts":    attempts,
        "success":     False,
        "status_code": None,
        "error":       None,
    }

    # ── فحص Webhook URL ──────────────────────────────────────────────────
    webhook_url = _resolve_webhook_url(event_type)
    if not webhook_url:
        err = f"Webhook URL فارغ — أضف WEBHOOK_UPDATE_PRICES في البيئة"
        logger.warning("outbox #%s: %s", event_id, err)
        mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
        result["error"] = err
        return result

    # ── تحويل payload_json إلى dict للإرسال عبر aiohttp ─────────────────
    try:
        payload_dict = json.loads(payload_str)
    except (json.JSONDecodeError, TypeError):
        payload_dict = {"raw": payload_str[:1000], "event_type": event_type}

    # ── طلب HTTP غير متزامن (aiohttp — لا يُجمِّد Event Loop) ──────────
    try:
        async with session.post(
            webhook_url,
            json=payload_dict,
            headers=_DISPATCH_HEADERS,
            timeout=_HTTP_TIMEOUT,
        ) as resp:
            status = resp.status
            result["status_code"] = status

            if 200 <= status < 300:
                # استخراج execution_id من رأس Make.com إذا توفّر
                exec_id = (
                    resp.headers.get("X-Execution-Id")
                    or resp.headers.get("X-Request-Id")
                    or f"http_{status}"
                )
                mark_outbox_sent(event_id, make_execution_id=exec_id, db_path=db_path)
                result["success"] = True
                logger.debug(
                    "outbox #%d → %s ✓ (HTTP %d)",
                    event_id, event_type, status,
                )

            elif 400 <= status < 500:
                # خطأ عميل — لا جدوى من إعادة المحاولة
                body = ""
                try:
                    body = (await resp.text())[:300]
                except Exception:
                    pass
                err = f"HTTP {status} (client error): {body}"
                mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
                result["error"] = err
                logger.warning("outbox #%d → %s ✗ HTTP %d", event_id, event_type, status)

            else:
                # 5xx أو غير متوقع — يُعاد المحاولة
                body = ""
                try:
                    body = (await resp.text())[:300]
                except Exception:
                    pass
                err = f"HTTP {status} (server error): {body}"
                mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
                result["error"] = err
                logger.warning("outbox #%d → %s ✗ HTTP %d", event_id, event_type, status)

    except asyncio.TimeoutError:
        err = "Timeout — Make.com لم يستجب في 20 ثانية"
        mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
        result["error"] = err
        logger.warning("outbox #%d Timeout", event_id)

    except aiohttp.ClientConnectionError as exc:
        err = f"ConnectionError: {str(exc)[:200]}"
        mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
        result["error"] = err
        logger.warning("outbox #%d ConnectionError: %s", event_id, exc)

    except aiohttp.ClientError as exc:
        err = f"aiohttp ClientError: {str(exc)[:200]}"
        mark_outbox_failed(event_id, error_msg=err, db_path=db_path)
        result["error"] = err
        logger.warning("outbox #%d ClientError: %s", event_id, exc)

    return result


# ══════════════════════════════════════════════════════════════════════════
#  3. dispatch_pending_events_async — الدالة الرئيسية
# ══════════════════════════════════════════════════════════════════════════
async def dispatch_pending_events_async(
    batch_size: int = 50,
    db_path:    str = None,
) -> Dict[str, Any]:
    """
    يُفرِّغ دفعة من outbox_events إلى Make.com Webhook بشكل غير متزامن.
    ══════════════════════════════════════════════════════════════════════
    الخطوات:
      1. يقرأ batch_size حدثاً (pending + failed < 3 محاولات) — FIFO.
      2. لكل حدث:
         أ. يُرسِل عبر aiohttp (لا يُجمِّد Event Loop).
         ب. 2xx → mark_outbox_sent.
         ج. غير 2xx / Timeout → mark_outbox_failed.
      3. await asyncio.sleep(0.5) إجباري بين كل إرسالين.
         (يمنع Rate-Limit Make.com)
    يُرجع dict إحصائي.
    """
    db_path = db_path or PERFUME_DB

    stats: Dict[str, Any] = {
        "read":        0,
        "sent":        0,
        "failed":      0,
        "skipped":     0,
        "started_at":  datetime.now().isoformat(),
        "finished_at": None,
    }

    events = get_pending_outbox_events(limit=batch_size, db_path=db_path)
    stats["read"] = len(events)

    if not events:
        logger.info("dispatch_pending_events_async: لا أحداث معلقة")
        stats["finished_at"] = datetime.now().isoformat()
        return stats

    logger.info(
        "dispatch_pending_events_async: بدأ إرسال %d حدث إلى Make.com",
        len(events),
    )

    connector = aiohttp.TCPConnector(ssl=False, limit=3)
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            for idx, event in enumerate(events):
                result = await _send_event(session, event, db_path=db_path)

                if result["success"]:
                    stats["sent"] += 1
                elif result.get("error") and "Webhook URL فارغ" in (result.get("error") or ""):
                    stats["skipped"] += 1
                else:
                    stats["failed"] += 1

                # ── Rate-Limit Make.com — await asyncio.sleep إجباري ────
                if idx < len(events) - 1:
                    await asyncio.sleep(_RATE_LIMIT_SLEEP)
    finally:
        stats["finished_at"] = datetime.now().isoformat()

    logger.info(
        "dispatch_pending_events_async: أُرسل=%d | فشل=%d | تخطّى=%d",
        stats["sent"], stats["failed"], stats["skipped"],
    )
    return stats
