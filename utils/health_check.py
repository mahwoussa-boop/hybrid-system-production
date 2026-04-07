"""
utils/health_check.py — الفحص الذاتي للنظام عند الإقلاع
══════════════════════════════════════════════════════════
يتحقق من:
  1. وجود المجلدات الأساسية وإمكانية الكتابة فيها
  2. إمكانية قراءة competitors_list.json
  3. اتصال Gemini API (ping خفيف — فقط إذا وُجد مفتاح)
  4. أن لا توجد قاعدة بيانات تالفة

المبدأ: صامت عند النجاح، تحذير (warning) عند الفشل — لا يوقف التطبيق أبداً.
"""
import json
import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class DiagnosticReport:
    """نتيجة الفحص الذاتي."""
    ok: bool = True
    warnings: list[str] = field(default_factory=list)
    errors:   list[str] = field(default_factory=list)
    details:  dict      = field(default_factory=dict)

    def warn(self, msg: str, key: str = "") -> None:
        self.warnings.append(msg)
        if key:
            self.details[key] = "⚠️ " + msg
        logger.warning("[health] %s", msg)

    def fail(self, msg: str, key: str = "") -> None:
        self.errors.append(msg)
        self.ok = False
        if key:
            self.details[key] = "❌ " + msg
        logger.error("[health] %s", msg)

    def pass_(self, key: str, msg: str = "") -> None:
        self.details[key] = "✅ " + (msg or key)


def _data_dir() -> str:
    return (os.environ.get("DATA_DIR") or "").strip() or "data"


def _resolve_data_path(filename: str) -> str:
    """
    يجد أول مسار موجود بالأولوية:
    1. DATA_DIR/filename   (Railway Volume)
    2. data/filename       (بجانب app.py — للتطوير المحلي)
    يُرجع المسار الأول الموجود، أو مسار DATA_DIR كافتراضي.
    """
    primary = os.path.join(_data_dir(), filename)
    if os.path.exists(primary):
        return primary
    # مسار احتياطي: data/ نسبياً من جذر المشروع
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    fallback = os.path.join(root, "data", filename)
    if os.path.exists(fallback):
        return fallback
    return primary  # إرجاع المسار الأساسي حتى لو غير موجود


def _check_directories(rep: DiagnosticReport) -> None:
    """مجلدات إلزامية: data/ و scrapers/."""
    for d, label in [(_data_dir(), "data/"), ("scrapers", "scrapers/")]:
        if os.path.isdir(d):
            # فحص الكتابة
            test_path = os.path.join(d, ".health_write_test")
            try:
                with open(test_path, "w") as f:
                    f.write("ok")
                os.remove(test_path)
                rep.pass_(f"dir_{label}", f"مجلد {label} موجود وقابل للكتابة")
            except OSError as exc:
                rep.warn(f"مجلد {label} موجود لكن لا يمكن الكتابة فيه: {exc}", f"dir_{label}")
        else:
            # محاولة الإنشاء
            try:
                os.makedirs(d, exist_ok=True)
                rep.pass_(f"dir_{label}", f"مجلد {label} أُنشئ للتو")
            except OSError as exc:
                rep.fail(f"تعذّر إنشاء مجلد {label}: {exc}", f"dir_{label}")


def _check_competitors_file(rep: DiagnosticReport) -> None:
    """
    competitors_list.json قابل للقراءة وغير فارغ.
    يبحث في DATA_DIR ثم في data/ المحلية.
    """
    path = _resolve_data_path("competitors_list.json")
    if not os.path.exists(path):
        # ليس خطأ حرجاً — المستخدم يضيف المتاجر من الواجهة
        rep.pass_("competitors_file",
                  "لا يوجد ملف متاجر بعد — أضف متاجرك من صفحة الكشط")
        return
    try:
        data = json.loads(open(path, encoding="utf-8").read())
        count = len(data) if isinstance(data, list) else 0
        rep.pass_("competitors_file", f"{count} متجر مُعرَّف ({path})")
    except Exception as exc:
        rep.warn(f"تعذّر قراءة competitors_list.json: {exc}", "competitors_file")


def _check_progress_writable(rep: DiagnosticReport) -> None:
    """التأكد من إمكانية الكتابة في scraper_progress.json.
    لا يُعيد الكتابة إذا كان الملف موجوداً — يحافظ على بيانات الكاشط الجارية."""
    path = os.path.join(_data_dir(), "scraper_progress.json")
    try:
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"running": False, "health_init": True}, f)
            rep.pass_("progress_file", "scraper_progress.json أُنشئ")
        else:
            # تحقق من إمكانية القراءة دون الكتابة (لا نمحو بيانات الكاشط)
            with open(path, "r", encoding="utf-8") as f:
                json.load(f)
            rep.pass_("progress_file", "scraper_progress.json موجود وقابل للقراءة")
    except Exception as exc:
        rep.warn(f"تعذّر التحقق من scraper_progress.json: {exc}", "progress_file")


def _check_gemini_api(rep: DiagnosticReport) -> None:
    """
    فحص مزودي AI — يقرأ من البيئة مباشرة لضمان القراءة اللحظية.
    غياب مفاتيح AI ليس خطأ حرجاً: يمكن رفع الملفات وتحليلها يدوياً بدون AI.
    """
    import os as _os_h
    has_gemini = bool(
        _os_h.environ.get("GEMINI_API_KEYS") or
        _os_h.environ.get("GEMINI_API_KEY") or
        _os_h.environ.get("GEMINI_KEY_1")
    )
    has_openrouter = bool(_os_h.environ.get("OPENROUTER_API_KEY") or
                          _os_h.environ.get("OPENROUTER_KEY"))
    has_cohere     = bool(_os_h.environ.get("COHERE_API_KEY"))

    # كذلك تحقق من secrets.toml (Streamlit Cloud)
    if not (has_gemini or has_openrouter or has_cohere):
        try:
            import streamlit as _st
            has_gemini = has_gemini or bool(
                _st.secrets.get("GEMINI_API_KEY") or _st.secrets.get("GEMINI_API_KEYS")
            )
            has_openrouter = has_openrouter or bool(_st.secrets.get("OPENROUTER_API_KEY"))
            has_cohere     = has_cohere     or bool(_st.secrets.get("COHERE_API_KEY"))
        except Exception:
            pass

    providers = []
    if has_gemini:      providers.append("Gemini")
    if has_openrouter:  providers.append("OpenRouter")
    if has_cohere:      providers.append("Cohere")

    if providers:
        rep.pass_("ai_providers", f"مزودو AI: {' · '.join(providers)}")
    else:
        rep.warn(
            "لم يُعثر على مفاتيح AI — ميزات التحليل الذكي معطّلة. "
            "أضف GEMINI_API_KEY أو OPENROUTER_API_KEY في إعدادات البيئة.",
            "ai_providers",
        )


def _check_database(rep: DiagnosticReport) -> None:
    """قاعدة البيانات SQLite قابلة للاتصال والكتابة."""
    try:
        from utils.db_manager import get_db, DB_PATH
        conn = get_db()
        conn.execute("SELECT 1").fetchone()
        # فحص الكتابة الفعلية (لا القراءة فقط)
        conn.execute("CREATE TABLE IF NOT EXISTS _health_ping (ts TEXT)")
        conn.execute("INSERT INTO _health_ping VALUES (?)", (str(time.time()),))
        conn.execute("DELETE FROM _health_ping WHERE 1=1")
        conn.commit()
        conn.close()
        rep.pass_("database", f"قاعدة البيانات SQLite سليمة — {DB_PATH}")
    except Exception as exc:
        rep.fail(f"قاعدة البيانات غير متاحة أو للقراءة فقط: {exc}", "database")


def run_system_diagnostics() -> DiagnosticReport:
    """
    الدالة الرئيسية — تُستدعى مرة واحدة عند إقلاع app.py.

    لا تُوقف التطبيق أبداً:
    - خطأ حرج   → errors[] + ok=False (الواجهة تعرض st.error)
    - تحذير      → warnings[] (الواجهة تعرض st.warning أو تتجاهل)
    - نجاح كامل → ok=True + صامت
    """
    t0 = time.monotonic()
    rep = DiagnosticReport()

    _check_directories(rep)
    _check_competitors_file(rep)
    _check_progress_writable(rep)
    _check_gemini_api(rep)
    _check_database(rep)

    elapsed = round(time.monotonic() - t0, 2)
    rep.details["elapsed_ms"] = f"{elapsed * 1000:.0f}ms"
    logger.info("[health] تشخيص اكتمل في %ss — ok=%s warnings=%d errors=%d",
                elapsed, rep.ok, len(rep.warnings), len(rep.errors))
    return rep
