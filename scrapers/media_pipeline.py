"""
scrapers/media_pipeline.py — مسار معالجة الصور غير المتزامن (Sprint 3)
═══════════════════════════════════════════════════════════════════════════
• asyncio.Queue محلي للتنسيق بين المنتج والعمال (بدون Redis/RabbitMQ)
• Pillow حصراً عبر asyncio.to_thread() — صفر خنق لـ Event Loop
• تحميل الصور بـ aiohttp + fallback sync عبر requests
• تحديث media_assets في SQLite عبر دوال db_manager (Sprint 1)

التشغيل المستقل:
  python -m scrapers.media_pipeline
  python -m scrapers.media_pipeline --concurrency 5 --limit 1000
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout

# ── anti_ban — نفس الطبقة المستخدمة في الكاشط ──────────────────────────
from scrapers.anti_ban import (
    fetch_with_retry,
    get_browser_headers,
    get_proxy_rotator,
)

# ── db_manager — دوال Sprint 1 ───────────────────────────────────────────
from utils.db_manager import (
    insert_media_asset,
    update_media_asset_status,
    get_unprocessed_staging_images,
    DB_PATH as PERFUME_DB,
)

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  إعدادات عامة
# ══════════════════════════════════════════════════════════════════════════
_ROOT      = Path(__file__).resolve().parent.parent
_MEDIA_DIR = (
    Path(os.environ.get("MEDIA_DIR", "")).resolve()
    if os.environ.get("MEDIA_DIR")
    else _ROOT / "data" / "media"
)
_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

JPEG_QUALITY  = 92      # جودة JPEG (0-100) — 92 توازن بين الحجم والجودة
MAX_WIDTH_PX  = 1200    # أقصى عرض — يُصغَّر إذا تجاوزه
MIN_WIDTH_PX  = 100     # أدنى عرض مقبول — يُرفض إذا أصغر
MAX_IMG_BYTES = 15 * 1024 * 1024   # 15 MB حد تحميل الصورة
_SENTINEL     = None    # إشارة إيقاف العمال


# ══════════════════════════════════════════════════════════════════════════
#  1. معالجة الصورة — مزامن | يُشغَّل حصراً عبر asyncio.to_thread()
# ══════════════════════════════════════════════════════════════════════════
def _sync_process_image(raw_bytes: bytes) -> Dict[str, Any]:
    """
    [مزامن — CPU-bound]
    يفتح الصورة الخام ويُحوِّلها إلى JPEG بجودة 92.
    WebP / AVIF / PNG / BMP → JPEG + تصغير إذا تجاوز MAX_WIDTH_PX.

    يُشغَّل حصراً عبر: await asyncio.to_thread(_sync_process_image, raw_bytes)
    لضمان عدم خنق Event Loop أثناء عمليات Pillow.

    يُرجع dict:
        ok (bool), jpeg_bytes, width, height, format, file_size, error
    """
    _EMPTY: Dict[str, Any] = {"ok": False, "jpeg_bytes": b"", "error": ""}

    try:
        from PIL import Image, UnidentifiedImageError  # type: ignore[import]
    except ImportError:
        return {**_EMPTY, "error": "Pillow غير مثبّت — pip install Pillow"}

    if not raw_bytes or len(raw_bytes) < 100:
        return {**_EMPTY, "error": "البيانات الخام فارغة أو أصغر من 100 بايت"}

    try:
        with Image.open(io.BytesIO(raw_bytes)) as img:
            original_format = img.format or "UNKNOWN"

            # تحويل إلى RGB (يُزيل قناة Alpha من WebP/AVIF/PNG)
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")

            w, h = img.size

            # رفض الصور الصغيرة جداً
            if w < MIN_WIDTH_PX:
                return {**_EMPTY, "error": f"عرض أصغر من الحد الأدنى: {w}px"}

            # تصغير نسبي إذا تجاوز الحد الأقصى
            if w > MAX_WIDTH_PX:
                ratio  = MAX_WIDTH_PX / w
                new_h  = max(1, int(h * ratio))
                img    = img.resize((MAX_WIDTH_PX, new_h), Image.LANCZOS)
                w, h   = img.size

            # ضغط إلى JPEG في buffer
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=JPEG_QUALITY, optimize=True)
            jpeg_bytes = buf.getvalue()

            return {
                "ok":          True,
                "jpeg_bytes":  jpeg_bytes,
                "width":       w,
                "height":      h,
                "format":      original_format,
                "file_size":   len(jpeg_bytes),
                "error":       "",
            }

    except Exception as exc:
        return {**_EMPTY, "error": str(exc)[:300]}


async def process_and_convert_image(raw_bytes: bytes) -> Dict[str, Any]:
    """
    [Async Wrapper — الاستدعاء الوحيد المسموح به من الكود الـ async]
    يُشغِّل _sync_process_image في Thread منفصل عبر asyncio.to_thread().
    يمنع تجميد Event Loop أثناء فك ضغط الصور (CPU-bound).
    """
    return await asyncio.to_thread(_sync_process_image, raw_bytes)


# ══════════════════════════════════════════════════════════════════════════
#  2. تحميل الصورة — aiohttp أولاً، ثم requests كـ fallback
# ══════════════════════════════════════════════════════════════════════════
def _sync_image_fallback(url: str) -> Optional[bytes]:
    """
    [مزامن] Fallback لتحميل الصورة بـ requests.content (bytes).
    مختلف عن try_all_sync_fallbacks الذي يُرجع str (HTML).
    يُشغَّل عبر asyncio.to_thread().
    """
    try:
        import requests as _req  # type: ignore[import]
        pr = get_proxy_rotator()
        proxy = pr.get_next()
        headers = get_browser_headers(referer=f"https://{urlparse(url).netloc}/")
        req_kwargs: Dict[str, Any] = {
            "headers":        headers,
            "timeout":        20,
            "verify":         False,
            "allow_redirects": True,
            "stream":         False,
        }
        if proxy:
            req_kwargs["proxies"] = {"http": proxy, "https": proxy}
        resp = _req.get(url, **req_kwargs)
        if resp.status_code == 200 and len(resp.content) > 500:
            if proxy:
                pr.mark_success(proxy)
            return resp.content
        if proxy and resp.status_code in (403, 407, 429):
            pr.mark_failed(proxy)
    except Exception as exc:
        logger.debug("_sync_image_fallback فشل '%s': %s", url, exc)
    return None


async def _download_image(
    session: aiohttp.ClientSession,
    image_url: str,
    referer: str = "",
) -> Optional[bytes]:
    """
    يُحمِّل الصورة الخام (bytes).
    الطبقة 1: aiohttp + fetch_with_retry (الأسرع، لا يخنق Event Loop).
    الطبقة 2: requests.content في asyncio.to_thread (fallback للمحمية).
    يُرجع bytes الخام أو None عند الفشل الكامل.
    """
    domain = urlparse(image_url).netloc

    # ── الطبقة 1: aiohttp ────────────────────────────────────────────────
    try:
        resp = await fetch_with_retry(
            session, image_url,
            max_retries=3,
            referer=referer or f"https://{domain}/",
        )
        if resp is not None:
            content_type = resp.headers.get("Content-Type", "")
            # نقبل image/* أو application/octet-stream
            if "image/" in content_type or "octet-stream" in content_type:
                raw = await resp.read()
                if raw and len(raw) > 500:
                    return raw[:MAX_IMG_BYTES]
    except Exception as exc:
        logger.debug("_download_image aiohttp '%s': %s", image_url, exc)

    # ── الطبقة 2: requests sync في thread ────────────────────────────────
    try:
        raw = await asyncio.to_thread(_sync_image_fallback, image_url)
        if raw and len(raw) > 500:
            return raw[:MAX_IMG_BYTES]
    except Exception as exc:
        logger.debug("_download_image fallback '%s': %s", image_url, exc)

    return None


# ══════════════════════════════════════════════════════════════════════════
#  3. حفظ الصورة محلياً
# ══════════════════════════════════════════════════════════════════════════
def _save_image_locally(jpeg_bytes: bytes, image_url: str, competitor: str) -> str:
    """
    يحفظ الـ JPEG في: _MEDIA_DIR/{competitor}/{YYYY-MM-DD}/{hash16}.jpg
    يُرجع المسار المطلق كـ string، أو '' عند الفشل.
    عملية صغيرة (كتابة bytes) — لا تحتاج asyncio.to_thread.
    """
    try:
        url_hash   = hashlib.sha256(image_url.encode("utf-8")).hexdigest()
        today      = datetime.now().strftime("%Y-%m-%d")
        safe_comp  = "".join(
            c for c in (competitor or "unknown") if c.isalnum() or c in "-_."
        )[:32]
        dest_dir   = _MEDIA_DIR / safe_comp / today
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path  = dest_dir / f"{url_hash[:16]}.jpg"
        dest_path.write_bytes(jpeg_bytes)
        return str(dest_path)
    except Exception as exc:
        logger.warning("_save_image_locally فشل '%s': %s", image_url, exc)
        return ""


# ══════════════════════════════════════════════════════════════════════════
#  4. دورة معالجة صورة واحدة
# ══════════════════════════════════════════════════════════════════════════
async def _process_single_image(
    session:    aiohttp.ClientSession,
    staging_id: Optional[int],
    image_url:  str,
    competitor: str,
    referer:    str,
) -> None:
    """
    الدورة الكاملة لمعالجة صورة واحدة:
    تحميل → process_and_convert_image (thread) → حفظ → تحديث DB.
    يُسجِّل الفشل في media_assets بشكل صامت (Graceful Degradation).
    """
    # ── تحقق مبدئي من الرابط ─────────────────────────────────────────────
    if not image_url or not image_url.startswith("http"):
        insert_media_asset(
            source_url=image_url or "invalid",
            staging_id=staging_id,
            is_valid=0,
            error_reason="رابط غير صالح أو لا يبدأ بـ http",
            db_path=PERFUME_DB,
        )
        return

    # ── تسجيل مبدئي (pending) في DB ──────────────────────────────────────
    insert_media_asset(
        source_url=image_url,
        staging_id=staging_id,
        is_valid=0,
        error_reason="قيد المعالجة",
        db_path=PERFUME_DB,
    )

    # ── تحميل ────────────────────────────────────────────────────────────
    raw_bytes = await _download_image(session, image_url, referer=referer)
    if not raw_bytes or len(raw_bytes) < 500:
        update_media_asset_status(
            source_url=image_url,
            is_valid=0,
            error_reason="فشل التحميل أو الحجم أقل من 500 بايت",
            db_path=PERFUME_DB,
        )
        logger.debug("فشل تحميل: %s", image_url)
        return

    # ── معالجة Pillow في Thread منفصل (إجباري بموجب Sprint 3) ────────────
    proc = await process_and_convert_image(raw_bytes)
    del raw_bytes  # تحرير الذاكرة فوراً

    if not proc.get("ok"):
        update_media_asset_status(
            source_url=image_url,
            is_valid=0,
            error_reason=proc.get("error", "فشل معالجة Pillow"),
            db_path=PERFUME_DB,
        )
        return

    # ── حفظ محلي ─────────────────────────────────────────────────────────
    local_path = _save_image_locally(proc["jpeg_bytes"], image_url, competitor)
    del proc["jpeg_bytes"]  # تحرير الذاكرة بعد الحفظ

    # ── تحديث DB بالحالة النهائية ─────────────────────────────────────────
    update_media_asset_status(
        source_url=image_url,
        is_valid=1,
        local_path=local_path,
        width_px=proc.get("width", 0),
        height_px=proc.get("height", 0),
        file_size_bytes=proc.get("file_size", 0),
        fmt=proc.get("format", "JPEG"),
        error_reason="",
        db_path=PERFUME_DB,
    )
    logger.debug(
        "✓ صورة: %s (%dx%d | %d KB)",
        image_url,
        proc.get("width", 0),
        proc.get("height", 0),
        proc.get("file_size", 0) // 1024,
    )


# ══════════════════════════════════════════════════════════════════════════
#  5. image_processor_worker — عامل الطابور
# ══════════════════════════════════════════════════════════════════════════
async def image_processor_worker(
    queue:     asyncio.Queue,
    session:   aiohttp.ClientSession,
    worker_id: int = 0,
) -> None:
    """
    عامل غير متزامن يسحب من asyncio.Queue باستمرار.
    يتوقف عند استقبال _SENTINEL.
    يستدعي queue.task_done() في كل الأحوال (نجاح أو فشل).
    """
    logger.debug("media-worker#%d: بدأ", worker_id)

    while True:
        item = await queue.get()

        if item is _SENTINEL:
            queue.task_done()
            logger.debug("media-worker#%d: استقبل SENTINEL — إيقاف", worker_id)
            break

        staging_id = item.get("staging_id")
        image_url  = item.get("image_url", "")
        competitor = item.get("competitor", "")
        referer    = item.get("referer", "")

        try:
            await _process_single_image(
                session, staging_id, image_url, competitor, referer
            )
        except Exception as exc:
            logger.warning(
                "media-worker#%d: خطأ غير معالَج '%s' — %s",
                worker_id, image_url, exc,
            )
            try:
                update_media_asset_status(
                    source_url=image_url,
                    is_valid=0,
                    error_reason=f"worker_error: {str(exc)[:200]}",
                    db_path=PERFUME_DB,
                )
            except Exception:
                pass
        finally:
            queue.task_done()


# ══════════════════════════════════════════════════════════════════════════
#  6. enqueue_pending_images — منتج الأحداث
# ══════════════════════════════════════════════════════════════════════════
async def enqueue_pending_images(
    queue: asyncio.Queue,
    limit: int = 500,
) -> int:
    """
    يقرأ الصور غير المعالجة (is_valid≠1) من raw_scrape_staging
    ويضخّها في الـ asyncio.Queue.
    يُرجع عدد العناصر المُدرجة في الطابور.
    """
    pending: List[Dict[str, Any]] = get_unprocessed_staging_images(
        limit=limit, db_path=PERFUME_DB
    )

    if not pending:
        logger.info("enqueue_pending_images: لا صور معلقة في raw_scrape_staging")
        return 0

    enqueued = 0
    for record in pending:
        image_url = (record.get("image_url") or "").strip()
        if not image_url:
            continue
        await queue.put({
            "staging_id": record.get("staging_id"),
            "image_url":  image_url,
            "competitor": record.get("competitor", ""),
            "referer":    record.get("url", ""),  # URL صفحة المنتج كـ Referer
        })
        enqueued += 1

    logger.info(
        "enqueue_pending_images: %d صورة في الطابور (من %d سجل)",
        enqueued, len(pending),
    )
    return enqueued


# ══════════════════════════════════════════════════════════════════════════
#  7. run_media_pipeline — المُنسِّق الرئيسي
# ══════════════════════════════════════════════════════════════════════════
async def run_media_pipeline(
    concurrency: int = 3,
    limit:       int = 500,
) -> Dict[str, Any]:
    """
    يُنسِّق دورة معالجة صور كاملة:
    1. ينشئ asyncio.Queue + N عامل.
    2. يضخّ الصور المعلقة من raw_scrape_staging.
    3. ينتظر اكتمال الطابور عبر queue.join().
    4. يوقف العمال بـ SENTINEL واحد لكل عامل.
    يُرجع dict بإحصاءات الدورة.

    نمط الإيقاف:
        for _ in workers: await queue.put(_SENTINEL)
        → كل عامل يُكمل مهمته الحالية قبل الإيقاف (Graceful Shutdown).
    """
    stats: Dict[str, Any] = {
        "enqueued":    0,
        "concurrency": concurrency,
        "limit":       limit,
        "started_at":  datetime.now().isoformat(),
        "finished_at": None,
        "error":       None,
    }

    # maxsize = concurrency × 4 يمنع تراكم الطابور في الذاكرة
    queue: asyncio.Queue = asyncio.Queue(maxsize=concurrency * 4)

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 2)
    timeout   = ClientTimeout(total=35, connect=10)
    headers   = get_browser_headers()

    try:
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=headers,
        ) as session:

            # ── تشغيل العمال ──────────────────────────────────────────────
            workers = [
                asyncio.create_task(
                    image_processor_worker(queue, session, worker_id=i),
                    name=f"media-worker-{i}",
                )
                for i in range(concurrency)
            ]

            # ── ضخّ الصور في الطابور (producer) ───────────────────────────
            stats["enqueued"] = await enqueue_pending_images(queue, limit=limit)

            # ── انتظار اكتمال جميع مهام الطابور ──────────────────────────
            await queue.join()

            # ── إيقاف العمال — SENTINEL لكل عامل ────────────────────────
            for _ in workers:
                await queue.put(_SENTINEL)
            await asyncio.gather(*workers, return_exceptions=True)

    except asyncio.CancelledError:
        logger.warning("run_media_pipeline: أُلغي (CancelledError)")
        stats["error"] = "CancelledError"
    except Exception as exc:
        logger.error("run_media_pipeline: فشل — %s", exc)
        stats["error"] = str(exc)

    stats["finished_at"] = datetime.now().isoformat()
    logger.info(
        "run_media_pipeline: اكتمل — %d صورة | concurrency=%d",
        stats["enqueued"], concurrency,
    )
    return stats


# ══════════════════════════════════════════════════════════════════════════
#  نقطة الدخول — التشغيل المباشر
# ══════════════════════════════════════════════════════════════════════════
def _setup_logging() -> None:
    root = logging.getLogger()
    if root.handlers:
        return
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%H:%M:%S")
    ch  = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    root.setLevel(logging.INFO)


def main() -> None:
    _setup_logging()
    parser = argparse.ArgumentParser(description="Media Pipeline Sprint 3")
    parser.add_argument("--concurrency", type=int, default=3,
                        help="عدد العمال المتوازيين (الافتراضي 3)")
    parser.add_argument("--limit", type=int, default=500,
                        help="أقصى عدد صور لكل دورة (الافتراضي 500)")
    args = parser.parse_args()

    # تهيئة DB قبل التشغيل
    try:
        from utils.db_manager import initialize_database
        initialize_database()
    except Exception as exc:
        logger.warning("تحذير: فشل تهيئة DB — %s", exc)

    stats = asyncio.run(
        run_media_pipeline(concurrency=args.concurrency, limit=args.limit)
    )
    print(f"\nالنتائج: {stats}")
    sys.exit(0 if not stats.get("error") else 1)


if __name__ == "__main__":
    main()
