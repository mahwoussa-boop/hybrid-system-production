"""
scrapers/scheduler.py — جدولة الكشط التلقائي v3.0 (Sprint 4)
═══════════════════════════════════════════════════════════════
يشغّل async_scraper.py كـ Orphan Process تلقائياً وفق الجدول المضبوط.

[v2 جديد]:
• كشف تلقائي للكاشط المتعطِّل (Crash Detection)
• استئناف تلقائي عند الانقطاع (--resume flag)
• سجلات منظَّمة مع تدوير الملفات (Log Rotation)

[v3 — Sprint 4]:
• Async Worker Loop في خيط daemon منفصل تماماً عن خيط الكاشط
• Outbox Dispatcher كل 5 دقائق  (dispatch_pending_events_async)
• Media Pipeline  كل 15 دقيقة  (run_media_pipeline)
• Staging Processor كل 30 دقيقة (process_staging_batch_async)
• لا time.sleep تحجب Event Loop — كل نوم عبر await asyncio.sleep

متغيرات البيئة:
  SCRAPE_INTERVAL_HOURS  → فاصل الكشط (افتراضي 12)
  STORE_CONCURRENCY      → متاجر متوازية (افتراضي 2)
  SCRAPER_MAX_PRODUCTS   → أقصى منتجات لكل متجر (0 = الكل)

الحالة محفوظة في: data/scheduler_state.json
"""
import asyncio
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── مسارات ─────────────────────────────────────────────────────────────────
_ROOT           = Path(__file__).resolve().parent.parent
_DATA_DIR       = (
    Path(os.environ.get("DATA_DIR", "")).resolve()
    if os.environ.get("DATA_DIR")
    else _ROOT / "data"
)
_STATE_FILE     = _DATA_DIR / "scheduler_state.json"
_PROGRESS_FILE  = _DATA_DIR / "scraper_progress.json"
_CHECKPOINT_FILE = _DATA_DIR / "scraper_checkpoint.json"
_SCRAPER_SCRIPT = _ROOT / "scrapers" / "async_scraper.py"

# ── الافتراضيات ─────────────────────────────────────────────────────────────
DEFAULT_INTERVAL_HOURS  = int(os.environ.get("SCRAPE_INTERVAL_HOURS", "12"))
DEFAULT_STORE_CONCURRENCY = int(os.environ.get("STORE_CONCURRENCY", "2"))
DEFAULT_MAX_PRODUCTS    = int(os.environ.get("SCRAPER_MAX_PRODUCTS", "0"))

# عتبة كشف التعطّل: إذا progress.json قال running=True
# لكن updated_at منذ أكثر من هذه الدقائق → نعتبره متعطِّلاً
_CRASH_THRESHOLD_MINUTES = 15


# ══════════════════════════════════════════════════════════════════════════
#  إدارة الحالة
# ══════════════════════════════════════════════════════════════════════════
def _load_state() -> dict:
    try:
        return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {
            "enabled": False,
            "next_run": None,
            "interval_hours": DEFAULT_INTERVAL_HOURS,
            "last_run": None,
            "runs_count": 0,
            "store_concurrency": DEFAULT_STORE_CONCURRENCY,
            "max_products": DEFAULT_MAX_PRODUCTS,
        }


def _save_state(state: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_scheduler_status() -> dict:
    """يُرجع حالة المجدول للعرض في الواجهة."""
    s = _load_state()
    now = datetime.utcnow()
    if s.get("next_run"):
        try:
            nxt = datetime.fromisoformat(s["next_run"])
            remaining = nxt - now
            seconds = max(0, int(remaining.total_seconds()))
            s["remaining_seconds"] = seconds
            s["next_run_label"] = _fmt_duration(seconds)
        except Exception:
            s["remaining_seconds"] = 0
            s["next_run_label"] = "—"
    else:
        s["remaining_seconds"] = 0
        s["next_run_label"] = "—"
    return s


def _fmt_duration(seconds: int) -> str:
    if seconds <= 0:
        return "الآن"
    h, r = divmod(seconds, 3600)
    m, s = divmod(r, 60)
    if h:
        return f"{h}س {m}د"
    if m:
        return f"{m}د {s}ث"
    return f"{s}ث"


def enable_scheduler(interval_hours: int = DEFAULT_INTERVAL_HOURS) -> None:
    """يُفعّل الجدولة التلقائية ويحسب أول تشغيل."""
    state = _load_state()
    state["enabled"]        = True
    state["interval_hours"] = interval_hours
    state["next_run"]       = (datetime.utcnow() + timedelta(hours=interval_hours)).isoformat()
    _save_state(state)
    logger.info(
        "المجدول مُفعَّل — كل %d ساعة، التشغيل القادم: %s",
        interval_hours, state["next_run"],
    )


def disable_scheduler() -> None:
    state = _load_state()
    state["enabled"]  = False
    state["next_run"] = None
    _save_state(state)
    logger.info("المجدول مُعطَّل")


# ══════════════════════════════════════════════════════════════════════════
#  كشف تعطّل الكاشط (Crash Detection) — v2 جديد
# ══════════════════════════════════════════════════════════════════════════
def _detect_crashed_scraper() -> bool:
    """
    يفحص progress.json:
    - إذا running=True لكن updated_at منذ أكثر من _CRASH_THRESHOLD_MINUTES → تعطّل
    - يُرجع True إذا اكتشف تعطّلاً ويوجد Checkpoint للاستئناف منه
    """
    try:
        prog = json.loads(_PROGRESS_FILE.read_text(encoding="utf-8"))
        if not prog.get("running", False):
            return False
        updated_at_str = prog.get("updated_at", "")
        if not updated_at_str:
            return False
        updated_at = datetime.fromisoformat(updated_at_str)
        age_minutes = (datetime.now() - updated_at).total_seconds() / 60
        if age_minutes < _CRASH_THRESHOLD_MINUTES:
            return False
        # تأكيد: يوجد Checkpoint للاستئناف؟
        checkpoint_exists = _CHECKPOINT_FILE.exists()
        logger.warning(
            "كشف تعطّل: الكاشط يدّعي أنه يعمل منذ %.0f دقيقة — checkpoint=%s",
            age_minutes, checkpoint_exists,
        )
        return checkpoint_exists
    except Exception:
        return False


def _mark_progress_crashed() -> None:
    """يُعلّم progress.json بأن الكاشط تعطّل (running=False)."""
    try:
        prog = json.loads(_PROGRESS_FILE.read_text(encoding="utf-8"))
        prog["running"] = False
        prog["last_error"] = f"تعطّل مكتشَف — استئناف تلقائي في {datetime.now().strftime('%H:%M:%S')}"
        prog["updated_at"] = datetime.now().isoformat()
        _PROGRESS_FILE.write_text(
            json.dumps(prog, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════
#  تشغيل الكاشط (Orphan Process)
# ══════════════════════════════════════════════════════════════════════════
def trigger_now(
    max_products: int = 0,
    concurrency: int = 8,
    full: bool = False,
    store_concurrency: int = DEFAULT_STORE_CONCURRENCY,
    resume: bool = False,
) -> bool:
    """
    يُشغّل الكاشط فوراً كـ Orphan Process في الخلفية.

    resume=True: يُمرّر --resume للكاشط للاستئناف من Checkpoint.
    store_concurrency: عدد المتاجر المتوازية.
    full=True: يتخطى lastmod cache ويكشط كل شيء.
    """
    if not _SCRAPER_SCRIPT.exists():
        logger.error("الكاشط غير موجود: %s", _SCRAPER_SCRIPT)
        return False
    try:
        cmd = [
            sys.executable, "-m", "scrapers.async_scraper",
            "--max-products", str(max_products),
            "--concurrency", str(concurrency),
            "--store-concurrency", str(store_concurrency),
        ]
        if full:
            cmd.append("--full")
        if resume:
            cmd.append("--resume")

        # توجيه المخرجات إلى ملف سجل مُدار — مع تدوير قديمة (أحدث 10 ملفات)
        _log_dir = _DATA_DIR / "logs"
        _log_dir.mkdir(parents=True, exist_ok=True)
        _ts_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        _log_file = _log_dir / f"scraper_{_ts_str}.log"

        # تنظيف السجلات القديمة (احتفظ بآخر 10 فقط)
        _rotate_logs(_log_dir, keep=10)

        with open(_log_file, "w", encoding="utf-8") as _lf:
            _lf.write(
                f"=== جلسة كشط جديدة ===\n"
                f"وقت البدء: {datetime.utcnow().isoformat()}\n"
                f"الأمر: {' '.join(cmd)}\n"
                f"الاستئناف: {'نعم' if resume else 'لا'}\n"
                f"{'=' * 40}\n\n"
            )
            proc = subprocess.Popen(
                cmd,
                stdout=_lf,
                stderr=_lf,
                start_new_session=True,
                cwd=str(_ROOT),
            )

        state = _load_state()
        state["last_run"]          = datetime.utcnow().isoformat()
        state["runs_count"]        = state.get("runs_count", 0) + 1
        state["last_log"]          = str(_log_file)
        state["last_pid"]          = proc.pid
        state["last_resume"]       = resume
        interval                   = state.get("interval_hours", DEFAULT_INTERVAL_HOURS)
        state["next_run"]          = (datetime.utcnow() + timedelta(hours=interval)).isoformat()
        _save_state(state)

        logger.info(
            "الكاشط انطلق (PID=%d) — التشغيل #%d — resume=%s — سجل: %s",
            proc.pid, state["runs_count"], resume, _log_file.name,
        )
        return True
    except Exception as exc:
        logger.error("فشل تشغيل الكاشط: %s", exc)
        return False


def _rotate_logs(log_dir: Path, keep: int = 10) -> None:
    """يحذف أقدم ملفات السجل ويحتفظ بآخر `keep` فقط."""
    try:
        logs = sorted(log_dir.glob("scraper_*.log"), key=lambda p: p.stat().st_mtime)
        for old_log in logs[:-keep]:
            old_log.unlink(missing_ok=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════
#  Daemon Thread — يفحص الجدول ويكشف التعطّل كل دقيقة
# ══════════════════════════════════════════════════════════════════════════
_scheduler_thread: Optional[threading.Thread] = None
_running = threading.Event()


def _scheduler_loop() -> None:
    """
    يعمل في خيط daemon — يفحص كل 60 ثانية:
    1. هل حان وقت الكشط المجدوَل؟
    2. هل تعطَّل الكاشط الأخير ويوجد Checkpoint للاستئناف؟
    """
    logger.info("خيط المجدول بدأ (v2 — مع كشف التعطّل)")
    while _running.is_set():
        try:
            state = _load_state()

            # ── فحص 1: كشف التعطّل والاستئناف التلقائي ──────────────────
            if _detect_crashed_scraper():
                logger.warning("تعطّل مكتشَف — استئناف تلقائي للكاشط من Checkpoint...")
                _mark_progress_crashed()
                trigger_now(
                    max_products=state.get("max_products", DEFAULT_MAX_PRODUCTS),
                    concurrency=state.get("concurrency", 8),
                    store_concurrency=state.get("store_concurrency", DEFAULT_STORE_CONCURRENCY),
                    resume=True,  # استئناف من Checkpoint
                )
                # انتظر فترة أطول بعد الاستئناف لإعطاء الكاشط وقتاً
                _running.wait(timeout=300)
                continue

            # ── فحص 2: الجدول الزمني الطبيعي ────────────────────────────
            if state.get("enabled") and state.get("next_run"):
                next_run = datetime.fromisoformat(state["next_run"])
                if datetime.utcnow() >= next_run:
                    logger.info(
                        "حان وقت الكشط التلقائي (التشغيل #%d) — أبدأ الآن...",
                        state.get("runs_count", 0) + 1,
                    )
                    trigger_now(
                        max_products=state.get("max_products", DEFAULT_MAX_PRODUCTS),
                        concurrency=state.get("concurrency", 8),
                        store_concurrency=state.get("store_concurrency", DEFAULT_STORE_CONCURRENCY),
                        full=state.get("full_mode", False),
                        resume=False,
                    )

        except Exception as exc:
            logger.debug("scheduler loop error: %s", exc)

        # انتظر 60 ثانية أو حتى يُلغى الـ event
        _running.wait(timeout=60)


def start_scheduler_thread() -> None:
    """
    يُشغّل خيط المجدول (الكاشط) + خيط العمال الـ async عند إقلاع التطبيق.
    آمن للاستدعاء المتعدد — لا يُنشئ خيطاً ثانياً.
    خيطان daemon مستقلان:
      scraper-scheduler : يراقب الكشط كل 60 ثانية (threading.Event)
      async-worker      : Outbox/Media/Staging (asyncio Event Loop)
    """
    global _scheduler_thread
    if not (_scheduler_thread and _scheduler_thread.is_alive()):
        _running.set()
        _scheduler_thread = threading.Thread(
            target=_scheduler_loop, name="scraper-scheduler", daemon=True
        )
        _scheduler_thread.start()
        logger.info("خيط المجدول بدأ (daemon) — فحص كل 60 ثانية")

    # تشغيل Async Worker (Outbox / Media / Staging)
    start_async_worker()


def stop_scheduler_thread() -> None:
    """يوقف خيط المجدول بأمان."""
    _running.clear()
    logger.info("خيط المجدول أُوقف")


# ══════════════════════════════════════════════════════════════════════════
#  Async Worker Loop — Sprint 4
#  خيط daemon منفصل تماماً بحلقة أحداث asyncio مستقلة.
#  يُشغِّل: Outbox (5د) / Media (15د) / Staging (30د)
#  لا يُؤثر على خيط الكاشط الموجود.
# ══════════════════════════════════════════════════════════════════════════
_async_thread:    Optional[threading.Thread]            = None
_async_loop:      Optional[asyncio.AbstractEventLoop]   = None
_async_stop_flag: threading.Event                       = threading.Event()

# فترات الجدولة (بالثواني)
_INTERVAL_OUTBOX   =  5 * 60    # 5 دقائق
_INTERVAL_MEDIA    = 15 * 60    # 15 دقيقة
_INTERVAL_STAGING  = 30 * 60    # 30 دقيقة
_INTERVAL_DB_OPT   = 24 * 60 * 60  # 24 ساعة (صيانة DB)

# تأخير الإقلاع الأولي (يمنح التطبيق وقتاً للتهيئة الكاملة)
_INIT_DELAY_OUTBOX   =  30   # ثانية
_INIT_DELAY_MEDIA    =  60
_INIT_DELAY_STAGING  =  90
_INIT_DELAY_DB_OPT   = 120   # ثانيتان إضافيتان — تعمل بعد اكتمال التهيئة


async def _loop_task(
    coro_fn,
    interval_sec:    int,
    task_name:       str,
    initial_delay:   int = 0,
) -> None:
    """
    يُشغِّل coro_fn كل interval_sec.
    يدعم الإيقاف النظيف عبر _async_stop_flag و asyncio.CancelledError.
    يُفحص _async_stop_flag كل 10 ثواني أثناء فترة الانتظار.
    """
    try:
        if initial_delay:
            await asyncio.sleep(initial_delay)
        while not _async_stop_flag.is_set():
            t_start = asyncio.get_event_loop().time()
            try:
                await coro_fn()
                logger.debug("async worker [%s]: دورة اكتملت", task_name)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.error("async worker [%s]: خطأ — %s", task_name, exc)

            # انتظار بدفعات 10 ثواني لدعم الإيقاف السريع
            elapsed   = asyncio.get_event_loop().time() - t_start
            remaining = max(0.0, interval_sec - elapsed)
            while remaining > 0 and not _async_stop_flag.is_set():
                chunk = min(10.0, remaining)
                await asyncio.sleep(chunk)
                remaining -= chunk

    except asyncio.CancelledError:
        logger.debug("async worker [%s]: أُلغي بأمان", task_name)


async def _async_worker_main() -> None:
    """
    الحلقة الرئيسية للعمال الـ async.
    تُشغِّل 3 مهام متوازية عبر asyncio.create_task.
    """
    # استيرادات داخلية لتجنب الاستيراد الدائري عند تحميل الموديول
    try:
        from utils.outbox_dispatcher import dispatch_pending_events_async
    except Exception as exc:
        logger.warning("scheduler: outbox_dispatcher غير محمَّل — %s", exc)
        dispatch_pending_events_async = None  # type: ignore[assignment]

    try:
        from scrapers.media_pipeline import run_media_pipeline
    except Exception as exc:
        logger.warning("scheduler: media_pipeline غير محمَّل — %s", exc)
        run_media_pipeline = None  # type: ignore[assignment]

    try:
        from engines.staging_processor import process_staging_batch_async
    except Exception as exc:
        logger.warning("scheduler: staging_processor غير محمَّل — %s", exc)
        process_staging_batch_async = None  # type: ignore[assignment]

    try:
        from utils.db_manager import optimize_database as _optimize_db
        async def _db_optimize_coro():
            """يُشغِّل optimize_database في thread — دالة SQLite مزامِنة."""
            await asyncio.to_thread(_optimize_db)
    except Exception as exc:
        logger.warning("scheduler: optimize_database غير محمَّل — %s", exc)
        _db_optimize_coro = None  # type: ignore[assignment]

    active_tasks = []

    if dispatch_pending_events_async:
        active_tasks.append(
            asyncio.create_task(
                _loop_task(
                    dispatch_pending_events_async,
                    _INTERVAL_OUTBOX,
                    "outbox",
                    initial_delay=_INIT_DELAY_OUTBOX,
                ),
                name="async-outbox",
            )
        )

    if run_media_pipeline:
        active_tasks.append(
            asyncio.create_task(
                _loop_task(
                    run_media_pipeline,
                    _INTERVAL_MEDIA,
                    "media",
                    initial_delay=_INIT_DELAY_MEDIA,
                ),
                name="async-media",
            )
        )

    if process_staging_batch_async:
        active_tasks.append(
            asyncio.create_task(
                _loop_task(
                    process_staging_batch_async,
                    _INTERVAL_STAGING,
                    "staging",
                    initial_delay=_INIT_DELAY_STAGING,
                ),
                name="async-staging",
            )
        )

    if _db_optimize_coro:
        active_tasks.append(
            asyncio.create_task(
                _loop_task(
                    _db_optimize_coro,
                    _INTERVAL_DB_OPT,
                    "db-optimize",
                    initial_delay=_INIT_DELAY_DB_OPT,
                ),
                name="async-db-optimize",
            )
        )

    if not active_tasks:
        logger.warning("scheduler async: لا مهام نشطة — الخيط سيخرج")
        return

    logger.info(
        "scheduler async: %d مهمة نشطة — %s",
        len(active_tasks),
        [t.get_name() for t in active_tasks],
    )

    try:
        await asyncio.gather(*active_tasks)
    except asyncio.CancelledError:
        for t in active_tasks:
            t.cancel()
        await asyncio.gather(*active_tasks, return_exceptions=True)


def _run_async_in_thread() -> None:
    """
    يُشغَّل في خيط daemon — ينشئ حلقة أحداث asyncio جديدة مستقلة.
    لا يشارك Event Loop مع Streamlit أو أي خيط آخر.
    """
    global _async_loop
    _async_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_async_loop)
    try:
        _async_loop.run_until_complete(_async_worker_main())
    except Exception as exc:
        logger.error("async worker loop انهار: %s", exc)
    finally:
        try:
            _async_loop.close()
        except Exception:
            pass
    logger.info("async worker loop أُغلق")


def start_async_worker() -> None:
    """
    يُشغّل خيط العمال الـ async (daemon).
    آمن للاستدعاء المتعدد — لا يُنشئ خيطاً ثانياً إذا كان يعمل.
    """
    global _async_thread
    if _async_thread and _async_thread.is_alive():
        logger.debug("start_async_worker: الخيط يعمل بالفعل — تخطّى")
        return
    _async_stop_flag.clear()
    _async_thread = threading.Thread(
        target=_run_async_in_thread,
        name="async-worker",
        daemon=True,
    )
    _async_thread.start()
    logger.info(
        "async worker بدأ (daemon) — Outbox/%dd | Media/%dd | Staging/%dd",
        _INTERVAL_OUTBOX // 60,
        _INTERVAL_MEDIA  // 60,
        _INTERVAL_STAGING // 60,
    )


def stop_async_worker() -> None:
    """
    يوقف خيط العمال الـ async بأمان.
    يُعلَّم _async_stop_flag ثم يُوقف Event Loop بـ call_soon_threadsafe.
    """
    global _async_loop
    _async_stop_flag.set()
    if _async_loop and _async_loop.is_running():
        _async_loop.call_soon_threadsafe(_async_loop.stop)
    logger.info("async worker أُوقف")
