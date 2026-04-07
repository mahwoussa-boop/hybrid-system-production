"""
app.py - نظام التسعير الذكي مهووس v26.0
# SYSTEM STATUS: LOCKED & AUTONOMOUS — Fire and Forget Mode
✅ معالجة خلفية مع حفظ تلقائي
✅ جداول مقارنة بصرية في كل الأقسام
✅ أزرار AI + قرارات لكل منتج
✅ بحث أسعار السوق والمنافسين
✅ بحث mahwous.com للمنتجات المفقودة
✅ تحديث تلقائي للأسعار عند إعادة رفع المنافس
✅ تصدير Make لكل منتج وللمجموعات
✅ Gemini Chat مباشر
✅ فلاتر ذكية في كل قسم
✅ تاريخ جميل لكل العمليات
✅ محرك أتمتة ذكي مع قواعد تسعير قابلة للتخصيص (v26.0)
✅ لوحة تحكم الأتمتة متصلة بالتنقل (v26.0)
✅ محرك كشط غير متزامن (Async Scraper + Detached Process)
✅ فحص ذاتي عند الإقلاع (Health Check)
✅ حفظ ملف المتجر الأساسي محلياً (Local Persistence v26.1)
"""
import html
import os
import streamlit as st
import pandas as pd
import threading
import time
import uuid
from datetime import datetime

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
from styles import (get_styles, vs_card, comp_strip, miss_card,
                    get_sidebar_toggle_js, lazy_img_tag, linked_product_title)
from engines.mahwous_core import validate_export_product_dataframe
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             smart_missing_barrier,
                             extract_brand, extract_size, extract_type, is_sample,
                             resolve_catalog_columns, detect_input_columns,
                             apply_user_column_map,
                             _first_image_url_from_row)
from engines.ai_engine import (call_ai, verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description, _parse_seo_json_block,
                                reclassify_review_items, ai_deep_analysis,
                                generate_salla_html_description,
                                generate_salla_brand_info,
                                visual_verify_match)
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff,
                            fetch_og_image_url, favicon_url_for_site,
                            fetch_page_title_from_url)
from utils.make_helper import (send_price_updates, send_new_products,
                                send_missing_products, send_single_product,
                                trigger_price_update,
                                verify_webhook_connection, export_to_make_format,
                                send_batch_smart)
from utils.salla_shamel_export import export_to_salla_shamel
from utils.filter_ui import (render_sidebar_filters, apply_global_filters,
                              get_active_filter_summary)
from utils.data_helpers import (safe_results_for_json, restore_results_from_json,
                                ts_badge, decision_badge,
                                row_media_urls_from_analysis,
                                our_product_url_from_row,
                                competitor_product_url_from_row,
                                format_missing_for_salla,
                                map_salla_categories,
                                validate_salla_brands,
                                upsert_competitors,
                                filter_unique_competitors)
from utils.data_paths import get_master_competitors_path
from utils.db_manager import (initialize_database, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               upsert_our_catalog, upsert_comp_catalog,
                               save_processed, get_processed, undo_processed,
                               get_processed_keys)

# ── مسار حفظ كتالوج المتجر الأساسي (Local Persistence) ──────────────────────
OUR_CATALOG_PATH = os.path.join("data", "saved_our_catalog.csv")


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_thumb_from_product_url(page_url: str) -> str:
    """صورة معاينة من صفحة المنتج عندما لا يوجد عمود صورة في الجدول المحفوظ."""
    u = (page_url or "").strip()
    if not u.startswith("http"):
        return ""
    og = fetch_og_image_url(u)
    if og:
        return og
    return favicon_url_for_site(u)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_title_from_product_url(page_url: str) -> str:
    """عنوان المنتج من og:title / <title> عندما يكون الاسم مخزّناً كرابط."""
    return fetch_page_title_from_url(page_url) or ""


# ── إعداد الصفحة ──────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)
st.markdown(get_sidebar_toggle_js(), unsafe_allow_html=True)

# ── فحص ذاتي عند الإقلاع (يعمل مرة واحدة فقط لكل جلسة) ────────────────
if "health_check_done" not in st.session_state:
    try:
        from utils.health_check import run_system_diagnostics
        _hc = run_system_diagnostics()
        st.session_state["health_check_done"] = True
        st.session_state["health_status"] = {
            "ok": _hc.ok,
            "warnings": _hc.warnings,
            "errors":   _hc.errors,
            "details":  _hc.details,
        }
    except Exception as _hce:
        st.session_state["health_check_done"] = True
        st.session_state["health_status"] = {
            "ok": True, "warnings": [], "errors": [], "details": {}
        }

# ══════════════════════════════════════════════════════════════════════════
#  نقطة إقلاع مركزية — مرة واحدة لكل عملية Streamlit
#  @st.cache_resource يضمن عدم التكرار عند إعادة تحميل الصفحة (rerun)
#  كلا الاستدعاءين آمنان للتكرار من الداخل (idempotent guards):
#    • initialize_database  ← _DB_INITIALIZED flag
#    • start_scheduler_thread ← thread.is_alive() check
# ══════════════════════════════════════════════════════════════════════════
@st.cache_resource
def _boot_system_once() -> dict:
    """
    نقطة إقلاع النظام الهجين (Sprint 1→4).
    تُشغَّل مرة واحدة لكل عملية Python — @st.cache_resource
    يمنعها من الإعادة عند كل rerun لصفحة Streamlit.

    الترتيب:
      1. initialize_database()    ← Sprint 1: الجداول + الترحيل
      2. start_scheduler_thread() ← Sprints 2-4: الكاشط + Async Worker
    """
    boot_result = {"db": False, "scheduler": False, "error": None}

    # 1. تهيئة قاعدة البيانات
    try:
        initialize_database()
        boot_result["db"] = True
    except Exception as _dbe:
        boot_result["error"] = str(_dbe)
        # الخطأ يُعرض في الـ UI أدناه — لا يُوقف الإقلاع

    # 2. تشغيل المجدول + Async Worker
    try:
        from scrapers.scheduler import start_scheduler_thread
        start_scheduler_thread()
        boot_result["scheduler"] = True
    except Exception as _sche:
        boot_result["error"] = boot_result.get("error") or str(_sche)

    return boot_result


_boot = _boot_system_once()
if _boot.get("error") and not _boot.get("db"):
    st.error(f"⚠️ خطأ حرج في تهيئة قاعدة البيانات: {_boot['error']}")

# أخطاء حرجة فقط تُعرض عالمياً (مثل DB تالفة)
_hs = st.session_state.get("health_status", {})
for _hc_err in _hs.get("errors", []):
    st.error(f"⚠️ فحص النظام: {_hc_err}")

# ── Session State ─────────────────────────
_defaults = {
    "results": None, "missing_df": None, "analysis_df": None,
    "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
    "nav_flash": None,    # رسالة انتقال سريعة من أزرار لوحة التحكم
    "last_audit_stats": None,  # عدادات تدقيق من run_full_analysis
    "_action_toast": None, # رسالة نجاح/فشل Callback تُعرض كـ toast
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# تحميل المنتجات المخفية من قاعدة البيانات عند كل تشغيل
_db_hidden = get_hidden_product_keys()
st.session_state.hidden_products = st.session_state.hidden_products | _db_hidden

# تنقل من أزرار لوحة التحكم — يُطبَّق هنا قبل `st.radio(..., key="main_nav")` في الشريط الجانبي
# (Streamlit يمنع تعيين st.session_state.main_nav بعد إنشاء الودجت في نفس التشغيل)
_nav_apply = st.session_state.pop("_nav_pending", None)
if _nav_apply and _nav_apply in SECTIONS:
    st.session_state.main_nav = _nav_apply

# ════════════════════════════════════════════════
#  دوال المعالجة — يجب تعريفها قبل استخدامها
# ════════════════════════════════════════════════
def _split_results(df):
    """تقسيم نتائج التحليل على الأقسام بأمان تام"""
    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))
    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved":    df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review":      df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "excluded":    df[_contains("القرار", "مستبعد")].reset_index(drop=True),
        "all":         df,
    }


def _merge_analysis_results(new_df: "pd.DataFrame", existing_df: "pd.DataFrame") -> "pd.DataFrame":
    """دمج نتائج تحليل جديدة مع نتائج سابقة.

    لكل منتج (معرَّف بـ معرف_المنتج أو اسمه) يُحتفظ بالمطابقة ذات أعلى نسبة تطابق.
    المنتجات المستبعدة في التحليل الجديد لا تُستبدل إذا كان لها مطابقة في القديم.
    """
    if existing_df is None or existing_df.empty:
        return new_df
    if new_df is None or new_df.empty:
        return existing_df

    import pandas as _pd

    def _key(row):
        pid = str(row.get("معرف_المنتج", "") or "").strip()
        if pid and pid not in ("nan", "None", "0", ""):
            return pid
        return str(row.get("المنتج", "") or "").strip().lower()

    # بناء dict من النتائج القديمة مفتاح → صف
    old_map = {}
    for _, r in existing_df.iterrows():
        k = _key(r)
        if k:
            old_map[k] = r

    merged_rows = []
    new_keys_seen = set()

    for _, r_new in new_df.iterrows():
        k = _key(r_new)
        new_keys_seen.add(k)
        score_new = float(r_new.get("نسبة_التطابق", 0) or 0)
        decision_new = str(r_new.get("القرار", "") or "")

        if k in old_map:
            r_old = old_map[k]
            score_old = float(r_old.get("نسبة_التطابق", 0) or 0)
            decision_old = str(r_old.get("القرار", "") or "")
            # احتفظ بالقديم إذا كان أفضل مطابقة (مستبعد لا يفوز على مطابقة حقيقية)
            is_new_excluded = "مستبعد" in decision_new
            is_old_excluded = "مستبعد" in decision_old
            if is_new_excluded and not is_old_excluded:
                merged_rows.append(r_old)
            elif not is_new_excluded and is_old_excluded:
                merged_rows.append(r_new)
            elif score_new >= score_old:
                merged_rows.append(r_new)
            else:
                merged_rows.append(r_old)
        else:
            merged_rows.append(r_new)

    # أضف المنتجات القديمة غير الموجودة في التحليل الجديد
    for k, r_old in old_map.items():
        if k not in new_keys_seen:
            merged_rows.append(r_old)

    if not merged_rows:
        return new_df

    result = _pd.DataFrame(merged_rows).reset_index(drop=True)
    return result


def _analysis_mask_for_review_row(adf: pd.DataFrame, row: pd.Series) -> pd.Series:
    """مفتاح مطابقة صف المراجعة مع جدول التحليل الكامل."""
    try:
        oid = str(row.get("معرف_المنتج", "") or "").strip()
        cid = str(row.get("معرف_المنافس", "") or "").strip()
        if oid and oid != "nan" and cid and cid != "nan":
            m = (adf["معرف_المنتج"].astype(str).str.strip() == oid) & (
                adf["معرف_المنافس"].astype(str).str.strip() == cid
            )
            if m.any():
                return m
        n1 = str(row.get("المنتج", "") or "").strip()
        n2 = str(row.get("منتج_المنافس", "") or "").strip()
        return (adf["المنتج"].astype(str).str.strip() == n1) & (
            adf["منتج_المنافس"].astype(str).str.strip() == n2
        )
    except Exception:
        return pd.Series([False] * len(adf))


def _reclassify_section_to_qarar(section: str):
    """يحوّل قيمة section بعد التطبيع في ai_engine إلى نص عمود القرار."""
    if not section:
        return None
    s = str(section)
    if "مراجعة" in s or s.strip() == "⚠️ تحت المراجعة":
        return None
    if "🔵" in s or ("مفقود" in s and "منتجات" not in s):
        return "🔍 منتجات مفقودة"
    if "🔴" in s or "أعلى" in s:
        return "🔴 سعر أعلى"
    if "🟢" in s or "أقل" in s:
        return "🟢 سعر أقل"
    if "✅" in s or "موافق" in s:
        return "✅ موافق"
    return None


def _apply_reclassify_to_analysis(adf: pd.DataFrame, review_df: pd.DataFrame,
                                  rc_results: list, min_conf: float = 75.0):
    """
    يحدّث عمود القرار في analysis_df حسب نتائج reclassify_review_items.
    يعيد (الجدول المحدث، إحصاءات).
    """
    stats = {
        "applied": 0, "skip_conf": 0, "skip_review": 0, "skip_idx": 0,
        "skip_no_row": 0, "skip_no_qarar": 0,
    }
    if adf is None or adf.empty or not rc_results:
        return adf, stats
    out = adf.copy()
    batch = review_df.head(30).reset_index(drop=True)
    nbatch = len(batch)
    for rc in rc_results:
        try:
            conf = float(rc.get("confidence") or 0)
        except Exception:
            conf = 0.0
        if conf < min_conf:
            stats["skip_conf"] += 1
            continue
        sec = rc.get("section", "")
        qarar = _reclassify_section_to_qarar(sec)
        if qarar is None:
            stats["skip_review"] += 1
            continue
        try:
            idx = int(rc.get("idx", 0) or 0)
        except Exception:
            idx = 0
        if idx < 1 or idx > nbatch:
            stats["skip_idx"] += 1
            continue
        row = batch.iloc[idx - 1]
        mask = _analysis_mask_for_review_row(out, row)
        if not mask.any():
            stats["skip_no_row"] += 1
            continue
        out.loc[mask, "القرار"] = qarar
        stats["applied"] += 1
    return out, stats


def _persist_analysis_after_reclassify(adf: pd.DataFrame):
    """يحدّث job_progress إن وُجد job_id وحالة done."""
    jid = st.session_state.get("job_id")
    if not jid:
        return
    try:
        job = get_job_progress(jid)
        if not job or str(job.get("status", "")) != "done":
            return
        miss = job.get("missing") if isinstance(job.get("missing"), list) else []
        save_job_progress(
            jid,
            int(job.get("total") or len(adf)),
            int(job.get("processed") or len(adf)),
            safe_results_for_json(adf.to_dict("records")),
            "done",
            str(job.get("our_file") or ""),
            str(job.get("comp_files") or ""),
            missing=miss,
        )
    except Exception:
        pass


# ── تحميل تلقائي للنتائج المحفوظة عند فتح التطبيق ──
# العلم _results_from_session: True = بيانات الجلسة الحالية | False = تحميل من جلسة سابقة
if "results_from_session" not in st.session_state:
    st.session_state["results_from_session"] = False

if st.session_state.results is None and not st.session_state.job_running:
    _auto_job = get_last_job()
    if _auto_job and _auto_job["status"] == "done" and _auto_job.get("results"):
        _auto_records = restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        if not _auto_df.empty:
            _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
            _auto_r = _split_results(_auto_df)
            _auto_r["missing"] = _auto_miss
            st.session_state.results     = _auto_r
            st.session_state.analysis_df = _auto_df
            st.session_state.job_id      = _auto_job.get("job_id")
            st.session_state["results_from_session"] = False
            # حفظ وقت التحليل السابق لعرضه في الواجهة
            st.session_state["results_loaded_at"] = _auto_job.get("updated_at", "")


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try: log_event(page, action, details)
    except: pass


def _effective_column_map(df: pd.DataFrame, key_prefix: str):
    """
    يقرأ اختيارات القوائم المنسدلة (إن وُجدت) وإلا يعود لنتيجة التعرف التلقائي.
    """
    if df is None or df.empty:
        return {"name": None, "price": None, "id_col": None, "img": None, "url": None}
    rc = resolve_catalog_columns(df)
    skip = "— (تخطي)"
    cols = {str(c) for c in df.columns}

    def _one(suffix: str, fallback_raw):
        k = f"{key_prefix}_{suffix}"
        v = st.session_state.get(k)
        fb = str(fallback_raw or "").strip()
        if v is None or v == skip:
            return fb if fb and fb in cols else None
        sv = str(v).strip()
        if sv == skip or sv not in cols:
            return fb if fb and fb in cols else None
        return sv

    return {
        "name": _one("name", rc.get("name")),
        "price": _one("price", rc.get("price")),
        "id_col": _one("id", rc.get("id")),
        "img": _one("img", rc.get("img")),
        "url": _one("url", rc.get("url")),
    }


def _render_column_mapping_expander(df: pd.DataFrame, key_prefix: str):
    """
    تحديد الأعمدة بقوائم منسدلة + معاينة صفوف قابلة للضبط + 5 قيم من عمود واحد.
    """
    if df is None or df.empty:
        st.warning("ملف فارغ أو غير مقروء")
        return
    rc = resolve_catalog_columns(df)
    cols_list = [str(c) for c in df.columns]
    skip = "— (تخطي)"
    options = [skip] + cols_list
    n_total = len(df)

    def _ix(fallback_raw):
        fb = str(fallback_raw or "").strip()
        if fb and fb in options:
            return options.index(fb)
        return 0

    st.caption(f"📊 **{len(cols_list)}** عمود — اضبط الأدوار أو اترك التعرف التلقائي")
    if len(cols_list) <= 4:
        st.caption("أسماء الأعمدة: " + "، ".join(f"«{c}»" for c in cols_list))
    g1, g2 = st.columns(2)
    with g1:
        st.selectbox("🏷️ اسم المنتج", options, index=_ix(rc.get("name")), key=f"{key_prefix}_name")
        st.selectbox("💰 السعر", options, index=_ix(rc.get("price")), key=f"{key_prefix}_price")
        st.selectbox("🔢 المعرف / SKU", options, index=_ix(rc.get("id")), key=f"{key_prefix}_id")
    with g2:
        st.selectbox("🖼️ صورة المنتج", options, index=_ix(rc.get("img")), key=f"{key_prefix}_img")
        st.selectbox("🔗 رابط المنتج", options, index=_ix(rc.get("url")), key=f"{key_prefix}_url")

    st.markdown("**عرض صفوف الملف**")
    pr1, pr2 = st.columns([1, 2])
    with pr1:
        n_preview = st.number_input(
            "عدد الصفوف",
            min_value=1,
            max_value=min(n_total, 500),
            value=min(5, n_total),
            step=1,
            key=f"{key_prefix}_preview_rows",
            help="معاينة من بداية الملف (كل الأعمدة).",
        )
    with pr2:
        st.caption(f"إجمالي الصفوف في الملف: **{n_total}**")
    _n = int(n_preview)
    st.dataframe(
        df.head(_n),
        use_container_width=True,
        height=min(520, 100 + _n * 28 + len(cols_list) * 2),
    )

    st.markdown("**معاينة — 5 قيم من عمود واحد**")
    peek_opts = ["— اختر عموداً —"] + cols_list
    pc = st.selectbox("العمود", peek_opts, key=f"{key_prefix}_peek")
    if pc and not str(pc).startswith("—"):
        try:
            st.dataframe(df[[pc]].head(5), use_container_width=True)
        except Exception:
            st.caption("تعذر عرض هذا العمود.")

    with st.expander("🔧 JSON — تفاصيل التعرف الخام", expanded=False):
        st.json(detect_input_columns(df))


def _validate_uploaded_catalog(df, label: str):
    """حارس أعمدة: اسم + سعر مطلوبان قبل التحليل (بعد read_file + التعرف العميق)."""
    if df is None or df.empty:
        st.error(f"⚠️ ملف فارغ أو غير مقروء: {label}")
        st.stop()
    m = resolve_catalog_columns(df)
    if not m.get("name") or not m.get("price"):
        st.error(
            f"⚠️ فشل التعرف الذكي على الأعمدة المطلوبة (**اسم المنتج** + **سعر**) في: **{label}**"
        )
        st.warning("معاينة خام — أول 10 صفوف:")
        st.dataframe(df.head(10), use_container_width=True)
        st.stop()


def _render_audit_bar(audit_stats: dict):
    """شريط تدقيق Zero Data Loss — يطابق المدخلات مع المخرجات المحاسَبة."""
    if not audit_stats:
        return
    ti  = int(audit_stats.get("total_input") or 0)
    pr  = int(audit_stats.get("processed") or 0)
    nc  = int(audit_stats.get("no_competitor_found") or 0)
    se  = int(audit_stats.get("skipped_empty") or 0)
    sk  = int(audit_stats.get("skipped_samples") or 0)
    cms = int(audit_stats.get("comp_market_size") or 0)  # إجمالي منتجات المنافسين
    tot = pr + nc + se + sk
    cms_html = (
        f'<div style="text-align:center;flex:1;min-width:88px;border-left:1px solid #4a6785;padding-left:10px">'
        f'<strong>📊 إجمالي منتجات السوق المستهدفة</strong><br>'
        f'<span style="font-size:1.5rem;color:#64b5f6;">{cms:,}</span></div>'
    ) if cms > 0 else ""
    st.markdown(
        f"""
    <div style="display:flex;flex-wrap:wrap;justify-content:space-between;gap:10px;
        background:#2c3e50;color:#fff;padding:15px;border-radius:8px;margin-bottom:16px;">
        <div style="text-align:center;flex:1;min-width:88px;"><strong>📦 إجمالي المدخلات</strong><br>
            <span style="font-size:1.5rem;">{ti}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>✅ وُجد منافس</strong><br>
            <span style="font-size:1.5rem;color:#4caf50;">{pr}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>⚪ لا منافس</strong><br>
            <span style="font-size:1.5rem;color:#ff9800;">{nc}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>👻 صفوف فارغة</strong><br>
            <span style="font-size:1.5rem;color:#9e9e9e;">{se}</span></div>
        <div style="text-align:center;flex:1;min-width:88px;"><strong>🚫 عينة / &lt;10مل</strong><br>
            <span style="font-size:1.5rem;color:#e53935;">{sk}</span></div>
        {cms_html}
    </div>
    """,
        unsafe_allow_html=True,
    )
    if ti > 0 and tot != ti:
        st.error(
            f"🚨 تحذير تدقيق: المدخلات ({ti}) لا تساوي مجموع الحالات ({tot}) — "
            f"معالج={pr} + بدون منافس={nc} + فارغ={se} + عينة/صغير={sk}."
        )


def _run_analysis_background(job_id, our_df, comp_dfs, our_file_name, comp_names):
    """تعمل في thread منفصل — تحفظ النتائج كل 10 منتجات مع حماية شاملة من الأخطاء"""
    total     = len(our_df)
    processed = 0
    _last_save = [0]  # آخر عدد تم حفظه (mutable لـ closure)

    def progress_cb(pct, current_results):
        nonlocal processed
        processed = int(pct * total)
        # حفظ كل 25 منتجاً أو عند الاكتمال (تقليل ضغط SQLite)
        if processed - _last_save[0] >= 25 or processed >= total:
            _last_save[0] = processed
            try:
                safe_res = safe_results_for_json(current_results)
                save_job_progress(
                    job_id, total, processed,
                    safe_res,
                    "running",
                    our_file_name, comp_names
                )
            except Exception as _save_err:
                # لا نوقف المعالجة بسبب خطأ حفظ جزئي
                import traceback
                traceback.print_exc()

    analysis_df = pd.DataFrame()
    missing_df  = pd.DataFrame()
    audit_stats = {}

    # ── المرحلة 1: التحليل الرئيسي ──────────────────────────────────
    try:
        analysis_df, audit_stats = run_full_analysis(
            our_df, comp_dfs,
            progress_callback=progress_cb
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # حفظ ما تم تحليله حتى الآن كنتائج جزئية
        save_job_progress(
            job_id, total, processed,
            [], f"error: تحليل المقارنة فشل — {str(e)[:200]}",
            our_file_name, comp_names
        )
        return

    # ── المرحلة 2: حفظ تاريخ الأسعار (لا يوقف المعالجة إذا فشل) ────
    try:
        for _, row in analysis_df.iterrows():
            if safe_float(row.get("نسبة_التطابق", 0)) > 0:
                upsert_price_history(
                    str(row.get("المنتج",       "")),
                    str(row.get("المنافس",       "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر",       0)),
                    safe_float(row.get("الفرق",        0)),
                    safe_float(row.get("نسبة_التطابق", 0)),
                    str(row.get("القرار",         ""))
                )
    except Exception:
        pass  # تاريخ الأسعار ثانوي — لا نوقف المعالجة

    # ── المرحلة 3: المنتجات المفقودة (منفصلة عن التحليل) ────────────
    try:
        raw_missing_df = find_missing_products(our_df, comp_dfs)
        missing_df = smart_missing_barrier(raw_missing_df, our_df)
    except Exception as e:
        import traceback
        traceback.print_exc()
        missing_df = pd.DataFrame()  # فشلت المفقودة لكن النتائج الرئيسية محفوظة

    # ── المرحلة 4: الحفظ النهائي ────────────────────────────────────
    try:
        safe_records = safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []

        save_job_progress(
            job_id, total, total,
            safe_records,
            "done",
            our_file_name, comp_names,
            missing=safe_missing,
            audit_stats=audit_stats,
        )
        log_analysis(
            our_file_name, comp_names, total,
            int((analysis_df.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
            len(missing_df)
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # محاولة أخيرة — حفظ بدون missing
        try:
            save_job_progress(
                job_id, total, total,
                safe_results_for_json(analysis_df.to_dict("records")),
                "done",
                our_file_name, comp_names,
                missing=[],
                audit_stats=audit_stats,
            )
        except Exception:
            save_job_progress(
                job_id, total, processed,
                [], f"error: فشل الحفظ النهائي — {str(e)[:200]}",
                our_file_name, comp_names
            )


def _find_analysis_row_for_processed(product_name: str):
    """
    يعيد صف التحليل المطابق لسجل «تمت المعالجة»: اسم منتجنا أو اسم المنتج عند المنافس.
    يبحث في analysis_df ثم في أقسام results (والجدول الكامل all).
    """
    pn = str(product_name or "").strip()
    if not pn:
        return None

    def _match_df(df):
        if df is None or getattr(df, "empty", True):
            return None
        for col in ("المنتج", "منتج_المنافس"):
            if col not in df.columns:
                continue
            try:
                m = df[df[col].astype(str).str.strip() == pn]
                if not m.empty:
                    return m.iloc[0]
            except Exception:
                continue
        return None

    adf = st.session_state.get("analysis_df")
    r = _match_df(adf)
    if r is not None:
        return r

    res = st.session_state.get("results") or {}
    for key in ("all", "price_raise", "price_lower", "approved", "review", "excluded", "missing"):
        r = _match_df(res.get(key))
        if r is not None:
            return r
    return None


def _lookup_images_from_analysis_session(product_name: str):
    """صورة منتجنا + صورة المنافس من جلسة التحليل أو أقسام النتائج."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return row_media_urls_from_analysis(row)
    except Exception:
        return "", ""


def _lookup_product_urls_from_analysis_session(product_name: str):
    """رابط منتجنا + رابط صفحة المنتج عند المنافس."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return our_product_url_from_row(row), competitor_product_url_from_row(row)
    except Exception:
        return "", ""


def _processed_dual_image_html(our_img: str, comp_img: str, title_our: str, title_comp: str) -> str:
    """خليتان للصور: منتجنا | المنافس — تحميل eager حتى تظهر فوراً في Streamlit."""
    w, h = 56, 56

    def _slot(label: str, url: str, alt: str) -> str:
        if url and str(url).strip():
            img = lazy_img_tag(url, w, h, alt, loading="eager")
        else:
            img = (
                f'<div style="width:{w}px;height:{h}px;border-radius:8px;background:#121c2e;'
                f'border:1px dashed #2a3f5f;display:flex;align-items:center;justify-content:center;'
                f'color:#4a5c78;font-size:.75rem">—</div>'
            )
        return (
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:5px;min-width:64px">'
            f'<span style="font-size:.68rem;color:#7eb8ff;font-weight:800;letter-spacing:.02em">{label}</span>'
            f"{img}</div>"
        )

    return (
        '<div style="display:flex;gap:16px;flex-shrink:0;align-items:flex-end;padding:2px 0">'
        f'{_slot("منتجنا", our_img, title_our[:40])}'
        f'{_slot("المنافس", comp_img, title_comp[:40])}'
        "</div>"
    )


def _is_http_url_text(s) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _humanize_competitor_upload(comp: str) -> str:
    """اسم ملف CSV/Excel → اسم متجر مقروء للعرض (بدون الامتداد)."""
    c = str(comp or "").strip()
    if not c:
        return "—"
    low = c.lower()
    for ext in (".csv", ".xlsx", ".xls", ".tsv", ".ods"):
        if low.endswith(ext):
            return c[: -len(ext)].strip() or c
    return c


def _display_name_for_missing_row(row) -> str:
    """
    اسم عرض للمفقودات: يفضّل نصاً حقيقياً من أي عمود معروف قبل اعتبار الاسم رابطاً فقط.
    """
    def _clean(v):
        x = str(v or "").strip()
        if not x or x.lower() in ("nan", "none", "<na>"):
            return ""
        return x

    for key in (
        "المنتج",
        "اسم المنتج",
        "اسم_المنتج",
        "Product",
        "Name",
        "name",
        "title",
        "الاسم",
        "منتج_المنافس",
    ):
        if key not in row.index:
            continue
        v = _clean(row.get(key))
        if v and not _is_http_url_text(v):
            return v

    br = _clean(row.get("الماركة"))
    sz = _clean(row.get("الحجم"))
    pt = _clean(row.get("النوع"))
    chunks = [c for c in (br, sz, pt) if c]
    if chunks:
        return " · ".join(chunks)

    return ""


def _processed_row_url_chips_html(our_url: str, comp_url: str) -> str:
    """روابط مختصرة بجانب سطر الملاحظات في «تمت المعالجة»."""
    parts = []
    ou = (our_url or "").strip()
    cu = (comp_url or "").strip()
    if ou.startswith("http"):
        parts.append(
            f'<a href="{html.escape(ou, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#4fc3f7;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 رابط منتجنا</a>'
        )
    if cu.startswith("http"):
        parts.append(
            f'<a href="{html.escape(cu, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#ff9800;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 عند المنافس</a>'
        )
    if not parts:
        return ""
    return '<span style="margin-right:8px">&nbsp;|&nbsp;</span>' + '<span style="margin:0 4px;color:#555">·</span>'.join(parts)


# ════════════════════════════════════════════════
#  Callbacks — أحداث الأزرار التفاعلية (Event-Driven)
#  تُعرَّف هنا (خارج حلقة الرسم) حتى تتوافق مع on_click.
#  ضمان: تُنفَّذ مرة واحدة بالضبط عند كل نقرة، والحالة تُحدَّث
#  تلقائياً قبل إعادة رسم الصفحة — بدون st.rerun() صريح.
# ════════════════════════════════════════════════
def _cb_send_make(
    prefix: str, idx,
    our_name: str, comp_name: str,
    our_price: float, comp_price: float, diff: float,
    decision: str, comp_src: str, pid: str, comp_url: str,
) -> None:
    """
    Callback: إرسال تحديث سعر واحد إلى Make.com عبر on_click.
    يقرأ السعر المستهدف من st.session_state لضمان القراءة اللحظية.
    """
    _price_key = f"target_price_{prefix}_{idx}"
    _tp = float(st.session_state.get(_price_key, 0) or 0)
    if _tp <= 0:
        st.session_state[f"_act_{prefix}_{idx}"] = (
            "error", "❌ السعر يجب أن يكون أكبر من صفر"
        )
        return
    _clean = str(pid).strip() if pid else ""
    if not _clean or _clean in ("nan", "None", "NaN", "0", "0.0"):
        st.session_state[f"_act_{prefix}_{idx}"] = (
            "error",
            f"❌ لا يمكن إرسال «{our_name}» — رقم المنتج (ID) مفقود. "
            "تأكد أن ملف كتالوجك يحتوي على عمود رقم المنتج.",
        )
        return

    _ok = trigger_price_update(
        pid, _tp, comp_url,
        name=our_name,
        comp_name=comp_name,
        comp_price=comp_price,
        diff=diff,
        decision=decision,
        competitor=comp_src,
    )

    _hk = f"{prefix}_{our_name}_{idx}"
    if _ok:
        st.session_state.hidden_products.add(_hk)
        try:
            save_hidden_product(_hk, our_name, "sent_to_make")
            save_processed(
                _hk, our_name, comp_src, "send_price",
                old_price=our_price, new_price=_tp, product_id=pid,
                notes=f"Make ← {prefix} | {comp_src} | {comp_price:.0f}→{_tp:.0f}ر.س",
            )
        except Exception:
            pass
        # toast يُعرض على مستوى الصفحة بعد إعادة الرسم
        st.session_state["_action_toast"] = (
            "success", f"✅ تم إرسال «{our_name}» ← {_tp:,.0f} ر.س"
        )
    else:
        st.session_state[f"_act_{prefix}_{idx}"] = (
            "error", "❌ فشل الإرسال — تحقق من الـ Webhook أو البيانات."
        )


def _cb_exclude(
    prefix: str, idx,
    our_name: str, our_price: float,
    comp_price: float, diff: float,
    comp_src: str, pid: str,
) -> None:
    """Callback: استبعاد المنتج وحفظه في DB عبر on_click."""
    st.session_state[f"excluded_{prefix}_{idx}"] = True
    st.session_state.hidden_products.add(f"{prefix}_{our_name}_{idx}")
    st.session_state.decisions_pending[our_name] = {
        "action": "removed", "reason": "استبعاد",
        "our_price": our_price, "comp_price": comp_price,
        "diff": diff, "competitor": comp_src,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    try:
        _hk = f"{prefix}_{our_name}_{idx}"
        log_decision(our_name, prefix, "removed", "استبعاد",
                     our_price, comp_price, diff, comp_src)
        save_hidden_product(_hk, our_name, "removed")
        save_processed(
            _hk, our_name, comp_src, "removed",
            old_price=our_price, new_price=our_price, product_id=pid,
            notes=f"استبعاد من {prefix}",
        )
    except Exception:
        pass


# ════════════════════════════════════════════════
#  مكوّن جدول المقارنة البصري (مشترك)
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True,
                     compact_cards=False, inline_filters=True):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية (مكشوفة في شبكة أو داخل Expander)
    - أزرار AI + قرار لكل منتج (Event-Driven via on_click)
    - تصدير Make
    - Pagination
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── تطبيق الفلاتر العالمية (Global Quick-Filters من الشريط الجانبي) ──
    df = apply_global_filters(df)
    if df.empty:
        _gf_sum = get_active_filter_summary()
        st.info(f"لا توجد منتجات تطابق الفلاتر الحالية ({_gf_sum})" if _gf_sum
                else "لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    opts = get_filter_options(df)
    if inline_filters:
        st.markdown(
            '<div class="filter-inline-wrap">'
            '<div class="filter-inline-title">🔍 فلاتر — بحث، ماركة، منافس، نوع</div></div>',
            unsafe_allow_html=True,
        )
        c1, c2, c3, c4 = st.columns([1.15, 1, 1, 1])
        search = c1.text_input("🔎 بحث", key=f"{prefix}_s")
        brand_f = c2.selectbox("🏷️ الماركة", opts["brands"], key=f"{prefix}_b")
        comp_f = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
        type_f = c4.selectbox("🧴 النوع", opts["types"], key=f"{prefix}_t")
        c5, c6, c7 = st.columns([1.2, 1, 1])
        match_min = c5.slider("أقل تطابق %", 0, 100, 0, key=f"{prefix}_m")
        price_min = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
        price_max = c7.number_input("سعر إلى", 0.0, key=f"{prefix}_p2")
    else:
        with st.expander("🔍 فلاتر متقدمة", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            search = c1.text_input("🔎 بحث", key=f"{prefix}_s")
            brand_f = c2.selectbox("🏷️ الماركة", opts["brands"], key=f"{prefix}_b")
            comp_f = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
            type_f = c4.selectbox("🧴 النوع", opts["types"], key=f"{prefix}_t")
            c5, c6, c7 = st.columns(3)
            match_min = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
            price_min = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
            price_max = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

    filters = {
        "search": search, "brand": brand_f, "competitor": comp_f,
        "type": type_f,
        "match_min": match_min if match_min > 0 else None,
        "price_min": price_min if price_min > 0 else 0.0,
        "price_max": price_max if price_max > 0 else None,
    }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (أول 20)",
                        "lower": "🤖 تحليل ذكي — رفع (أول 20)",
                        "review": "🤖 تحقق جماعي (أول 20)",
                        "approved": "🤖 مراجعة (أول 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (أول 20)"), key=f"{prefix}_bulk"):
            with st.spinner("🤖 AI يحلل البيانات..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in filtered.head(20).iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if section_type == "excluded":
            st.caption("إرسال Make غير متاح لهذا القسم")
        elif st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            if section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            if res["success"]:
                st.success(res["message"])
                # v26: سجّل كل منتج في processed_products
                for _i, (_idx, _r) in enumerate(filtered.iterrows()):
                    _pname = str(_r.get("المنتج", _r.get("منتج_المنافس", "")))
                    _pkey  = f"{prefix}_{_pname}_{_i}"
                    _pid_r = str(_r.get("معرف_المنتج", _r.get("معرف_المنافس", "")))
                    _comp  = str(_r.get("المنافس",""))
                    _op    = safe_float(_r.get("السعر", _r.get("سعر_المنافس", 0)))
                    _np    = safe_float(_r.get("سعر_المنافس", _r.get("السعر", 0)))
                    st.session_state.hidden_products.add(_pkey)
                    save_hidden_product(_pkey, _pname, "sent_to_make_bulk")
                    save_processed(_pkey, _pname, _comp, "send_price",
                                   old_price=_op, new_price=_np,
                                   product_id=_pid_r,
                                   notes=f"إرسال جماعي ← {prefix}")
                st.rerun()
            else:
                st.error(res["message"])
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            # v26: سجّل القرارات المعلقة في processed_products
            for k, v in pending.items():
                _pkey = f"decision_{k}"
                _act  = v.get("action","approved")
                save_processed(_pkey, k, v.get("competitor",""), _act,
                               old_price=safe_float(v.get("our_price",0)),
                               new_price=safe_float(v.get("comp_price",0)),
                               notes=f"قرار معلق → Make | {v.get('reason','')}")
            st.session_state.decisions_pending = {}
            st.rerun()

    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 20 if (compact_cards and prefix == "raise") else 25
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages > 1:
        page_num = st.number_input("الصفحة", 1, total_pages, 1, key=f"{prefix}_pg")
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]

       # ── الجدول البصري ─────────────────────
    for idx, row in page_df.iterrows():
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        if prefix in ("raise", "lower") and st.session_state.get(f"excluded_{prefix}_{idx}"):
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("نسبة_التطابق", 0))
        comp_src   = str(row.get("المنافس", ""))
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")
        _is_excluded = "مستبعد" in decision
        _vs_border = "#9e9e9e" if _is_excluded else None
        _vs_row_bg = "rgba(245,245,245,0.07)" if _is_excluded else None

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        _our_img_v, _comp_img_v = row_media_urls_from_analysis(row)
        _comp_url_v = competitor_product_url_from_row(row)
        _our_url_v = our_product_url_from_row(row)
        _price_alert = str(row.get("حالة_السعر", "") or "").strip()

        # بطاقة VS مع رقم المنتج + صور (lazy) عند توفرها — وضع مضغوط لقسم «سعر أعلى»
        _vs_compact = bool(compact_cards and prefix == "raise")
        _vs_html = vs_card(our_name, our_price, comp_name,
                           comp_price, diff, comp_src, _pid_str,
                           our_img=_our_img_v, comp_img=_comp_img_v,
                           comp_url=_comp_url_v, our_url=_our_url_v,
                           accent_border=_vs_border, row_bg=_vs_row_bg,
                           price_alert=_price_alert,
                           compact=_vs_compact)
        st.markdown(_vs_html, unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تاريخ آخر تغيير سعر
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            old_p = ph[1]["price"]
            chg = ph[0]["price"] - old_p
            chg_c = "#FF1744" if chg > 0 else "#00C853"
            price_change_html = f'<span style="color:{chg_c};font-size:.7rem">{"▲" if chg>0 else "▼"}{abs(chg):.0f} منذ {ph[1]["date"]}</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # ── 🤖 تبرير AI (Chain-of-Thought) — يظهر كـ Expander أنيق ────────
        _ai_reason = str(row.get("تبرير_AI", "") or "").strip()
        if _ai_reason:
            with st.expander("🧠 تبرير AI", expanded=False):
                st.markdown(
                    f'<div style="background:rgba(108,99,255,.10);border:1px solid rgba(108,99,255,.35);'
                    f'border-radius:8px;padding:8px 12px;font-size:.82rem;color:#c5c2ff;line-height:1.6">'
                    f'<span style="color:#6C63FF;font-weight:700">🔍 سبب المطابقة:</span> '
                    f'{_ai_reason}</div>',
                    unsafe_allow_html=True,
                )

        # شريط المنافسين المصغر — منقّى (1 منافس = أفضل مطابقة واحدة فقط)
        all_comps = row.get("جميع_المنافسين", row.get("جميع المنافسين", []))
        all_comps = filter_unique_competitors(all_comps)
        if len(all_comps) > 0:
            st.markdown(comp_strip(all_comps), unsafe_allow_html=True)

        # ── شريط الإجراءات التفاعلي (Event-Driven via on_click) ─────────
        if prefix in ("raise", "lower"):
            st.write("")
            _suggested = float(comp_price) - 1.0 if comp_price > 0 else float(our_price)
            if _suggested <= 0:
                _suggested = float(our_price)

            # pid يُحسب هنا لأنه مطلوب كـ arg للـ Callbacks
            _pid_cb_raw = (
                row.get("معرف_المنتج", "") or row.get("product_id", "")
                or row.get("رقم المنتج", "") or row.get("رقم_المنتج", "")
                or row.get("معرف المنتج", "") or ""
            )
            try:
                _fv_cb = float(_pid_cb_raw)
                _pid_cb = str(int(_fv_cb)) if _fv_cb == int(_fv_cb) else str(_pid_cb_raw)
            except (ValueError, TypeError):
                _pid_cb = str(_pid_cb_raw).strip()
            if _pid_cb in ("nan", "None", "NaN", ""):
                _pid_cb = ""

            _comp_url_make = (_comp_url_v or str(row.get("رابط_المنافس", "") or "")).strip()

            act_col1, act_col2, act_col3, act_col4 = st.columns([2.5, 2.5, 1.7, 1.7])
            with act_col1:
                st.number_input(
                    "🎯 السعر المستهدف (ر.س)",
                    value=float(_suggested),
                    min_value=0.0,
                    step=1.0,
                    key=f"target_price_{prefix}_{idx}",
                )
            with act_col2:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                st.button(
                    "🚀 تحديث السعر (Make)",
                    key=f"send_make_{prefix}_{idx}",
                    type="primary",
                    width='stretch',
                    on_click=_cb_send_make,
                    args=(
                        prefix, idx, our_name, comp_name,
                        our_price, comp_price, diff,
                        decision, comp_src, _pid_cb, _comp_url_make,
                    ),
                )
            with act_col3:
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                st.button(
                    "🗑️ استبعاد",
                    key=f"reject_bar_{prefix}_{idx}",
                    width='stretch',
                    on_click=_cb_exclude,
                    args=(
                        prefix, idx, our_name, our_price,
                        comp_price, diff, comp_src, _pid_cb,
                    ),
                )
            with act_col4:
                # ── 👁️ زر الفحص البصري (Hawk-Eye Vision) ─────────────────
                st.markdown("<div style='margin-top: 28px;'></div>", unsafe_allow_html=True)
                _vkey = f"_vision_{prefix}_{idx}"
                if st.button(
                    "👁️ فحص بصري",
                    key=f"vision_btn_{prefix}_{idx}",
                    width='stretch',
                    help="تحقق بصري عبر Gemini Vision: هل صورة منتجنا وصورة المنافس لنفس العطر؟",
                ):
                    with st.spinner("🦅 Hawk-Eye يحلل الصورتين…"):
                        try:
                            _vr = visual_verify_match(
                                _our_img_v, _comp_img_v, our_name
                            )
                            st.session_state[_vkey] = _vr
                        except Exception as _ve:
                            st.session_state[_vkey] = {
                                "match": False,
                                "reason": f"خطأ داخلي: {_ve}",
                                "source": "fallback",
                            }

            # عرض نتيجة الفحص البصري إن وُجدت
            _vision_res = st.session_state.get(_vkey)
            if _vision_res:
                _v_match  = _vision_res.get("match", False)
                _v_reason = _vision_res.get("reason", "")
                _v_src    = _vision_res.get("source", "")
                _v_bg     = "rgba(0,200,83,.10)" if _v_match else "rgba(255,71,87,.10)"
                _v_bdr    = "#00C853"             if _v_match else "#FF1744"
                _v_icon   = "✅ تطابق بصري مؤكد" if _v_match else "❌ صور مختلفة"
                _v_src_lbl = "🤖 Gemini Vision" if _v_src == "gemini_vision" else "⚠️ لم يكتمل الفحص"
                st.markdown(
                    f'<div style="background:{_v_bg};border:1px solid {_v_bdr};'
                    f'border-radius:8px;padding:8px 14px;margin:6px 0;font-size:.82rem">'
                    f'<b style="color:{"#00C853" if _v_match else "#FF5252"}">{_v_icon}</b>'
                    f'<span style="color:#aaa"> — {_v_reason}</span>'
                    f'<span style="float:left;color:#666;font-size:.7rem">{_v_src_lbl}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # عرض نتيجة الإجراء (خطأ فقط؛ النجاح يُعرض كـ toast أعلى الصفحة)
            _act_res = st.session_state.pop(f"_act_{prefix}_{idx}", None)
            if _act_res:
                _atype, _amsg = _act_res
                st.error(_amsg) if _atype == "error" else st.success(_amsg)

            _hr_act = (
                '<hr style="border:none;border-top:1px solid #2a2a3d;margin:10px 0 14px">'
                if _vs_compact
                else "<hr style='margin:16px 0;border-top:2px dashed rgba(238,238,238,.25);'>"
            )
            st.markdown(_hr_act, unsafe_allow_html=True)

        if prefix in ("raise", "lower"):
            b1, b2, b3, b4, b8, b9 = st.columns([1, 1, 1, 1, 1, 1])
        elif prefix == "approved":
            # قسم الموافقات: بدون تكرار «تحقق» الثاني ولا «تاريخ» — يبقى 🤖 تحقق في b1
            b1, b2, b3, b4, b5, b6, b7 = st.columns(7)
        else:
            b1, b2, b3, b4, b5, b6, b7, b8, b9 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_{idx}"):
                with st.spinner("🤖 AI يحلل ويتحقق..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r.get("success"):
                        icon = "✅" if r.get("match") else "❌"
                        conf = r.get("confidence", 0)
                        reason = r.get("reason","")[:200]
                        correct_sec = r.get("correct_section","")
                        suggested_price = r.get("suggested_price", 0)

                        # تحديد القسم الحالي من prefix
                        current_sec_map = {
                            "raise": "🔴 سعر أعلى",
                            "lower": "🟢 سعر أقل",
                            "approved": "✅ موافق",
                            "review": "⚠️ تحت المراجعة",
                            "excluded": "⚪ مستبعد (لا يوجد تطابق)",
                        }
                        current_sec = current_sec_map.get(prefix, "")

                        # هل AI يوافق على القسم الحالي؟
                        section_ok = True
                        if correct_sec and current_sec:
                            # مقارنة مبسطة
                            if ("اعلى" in correct_sec or "أعلى" in correct_sec) and prefix != "raise":
                                section_ok = False
                            elif ("اقل" in correct_sec or "أقل" in correct_sec) and prefix != "lower":
                                section_ok = False
                            elif "موافق" in correct_sec and prefix != "approved":
                                section_ok = False
                            elif ("مفقود" in correct_sec or "🔵" in correct_sec) and r.get("match") == False:
                                section_ok = False

                        if r.get("match"):
                            # مطابقة صحيحة — عرض نتيجة السعر
                            diff_info = ""
                            if prefix == "raise":
                                diff_info = f"\n\n💡 **توصية:** {'خفض السعر' if diff > 20 else 'إبقاء السعر'}"
                            elif prefix == "lower":
                                diff_info = f"\n\n💡 **توصية:** {'رفع السعر' if abs(diff) > 20 else 'إبقاء السعر'}"
                            if suggested_price > 0:
                                diff_info += f"\n💰 **السعر المقترح: {suggested_price:,.0f} ر.س**"

                            st.success(f"{icon} **تطابق {conf}%** — المطابقة صحيحة\n\n{reason}{diff_info}")

                            if not section_ok:
                                st.warning(f"⚠️ AI يرى أن هذا المنتج يجب أن يكون في قسم: **{correct_sec}**")
                        else:
                            # مطابقة خاطئة — تنبيه
                            st.error(f"{icon} **المطابقة خاطئة** ({conf}%)\n\n{reason}")
                            st.warning("🔵 هذا المنتج يجب أن يكون في **المنتجات المفقودة**")
                    else:
                        st.error("فشل AI")

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_{idx}"):
                with st.spinner("🌐 يبحث في السوق السعودي..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp  = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")[:250]
                        web_ctx = r.get("web_context","")
                        comps = r.get("competitors", [])
                        conf = r.get("confidence", 0)

                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = "✅ سعرنا ضمن السوق" if our_price <= mp * 1.1 else "⚠️ سعرنا أعلى من السوق — يُنصح بالخفض"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"💰 فرصة رفع ~{_gap:.0f} ر.س" if _gap > 10 else "✅ سعرنا قريب من السوق"

                        _comps_txt = ""
                        if comps:
                            _comps_txt = "\n\n**منافسون:**\n" + "\n".join(
                                f"• {c.get('name','')}: {c.get('price',0):,.0f} ر.س" for c in comps[:3]
                            )

                        _price_range = f"{rng.get('min',0):.0f}–{rng.get('max',0):.0f}" if rng else "—"
                        st.info(
                            f"💹 **سعر السوق: {mp:,.0f} ر.س** ({_price_range} ر.س)\n\n"
                            f"{rec}{_comps_txt}\n\n{'**' + _verdict + '**' if _verdict else ''}"
                        )
                        if web_ctx:
                            with st.expander("🔍 مصادر البحث"):
                                st.caption(web_ctx)
                    else:
                        st.warning("تعذر البحث في السوق")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                _hk3 = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk3)
                save_hidden_product(_hk3, our_name, "approved")
                save_processed(_hk3, our_name, comp_src, "approved",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"موافق من {prefix} | منافس: {comp_src}")
                st.rerun()

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        if prefix not in ("raise", "lower"):
            with b5:  # إزالة
                if st.button("🗑️ إزالة", key=f"rm_{prefix}_{idx}"):
                    st.session_state.decisions_pending[our_name] = {
                        "action": "removed", "reason": "إزالة",
                        "our_price": our_price, "comp_price": comp_price,
                        "diff": diff, "competitor": comp_src,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                    log_decision(our_name, prefix, "removed",
                                 "إزالة", our_price, comp_price, diff, comp_src)
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "removed")
                    save_processed(_hk, our_name, comp_src, "removed",
                                   old_price=our_price, new_price=our_price,
                                   product_id=str(row.get("معرف_المنتج","")),
                                   notes=f"إزالة من {prefix}")
                    st.rerun()

            with b6:  # سعر يدوي
                _auto_price_row = round(comp_price - 1, 2) if comp_price > 0 else our_price
                _custom_price = st.number_input(
                    "سعر", value=_auto_price_row, min_value=0.0,
                    step=1.0, key=f"cp_{prefix}_{idx}",
                    label_visibility="collapsed"
                )

            with b7:  # تصدير Make
                if st.button("📤 Make", key=f"mk_{prefix}_{idx}"):
                    _pid_raw = (
                        row.get("معرف_المنتج", "") or
                        row.get("product_id", "") or
                        row.get("رقم المنتج", "") or
                        row.get("رقم_المنتج", "") or
                        row.get("معرف المنتج", "") or ""
                    )
                    try:
                        _fv = float(_pid_raw)
                        _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                    except (ValueError, TypeError):
                        _pid = str(_pid_raw).strip()
                    if _pid in ("nan", "None", "NaN", ""):
                        _pid = ""
                    _final_price = _custom_price if _custom_price > 0 else _auto_price_row
                    if not _pid:
                        st.error(
                            f"❌ رقم المنتج (ID) مفقود — «{our_name}»\n"
                            "تأكد أن ملف كتالوجك يحتوي على عمود رقم المنتج (No. أو معرف المنتج)."
                        )
                        st.stop()
                    res = send_single_product({
                        "product_id": _pid,
                        "name": our_name, "price": _final_price,
                        "comp_name": comp_name, "comp_price": comp_price,
                        "diff": diff, "decision": decision, "competitor": comp_src
                    })
                    if res["success"]:
                        _hk = f"{prefix}_{our_name}_{idx}"
                        st.session_state.hidden_products.add(_hk)
                        save_hidden_product(_hk, our_name, "sent_to_make")
                        save_processed(_hk, our_name, comp_src, "send_price",
                                       old_price=our_price, new_price=_final_price,
                                       product_id=_pid,
                                       notes=f"Make ← {prefix} | منافس: {comp_src} | {comp_price:.0f}→{_final_price:.0f}ر.س")
                        st.rerun()

        if prefix != "approved":
            with b8:  # تحقق AI — يُصحح القسم (مكرر مع b1 في «موافق» فأُزيل من قسم الموافقات)
                if st.button("🔍 تحقق", key=f"vrf_{prefix}_{idx}"):
                    with st.spinner("🤖 يتحقق..."):
                        _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                        if _vr2.get("success"):
                            _mc2 = "✅ متطابق" if _vr2.get("match") else "❌ غير متطابق"
                            _conf2 = _vr2.get("confidence",0)
                            _sec2 = _vr2.get("correct_section","")
                            _reason2 = _vr2.get("reason","")[:150]
                            st.markdown(f"{_mc2} {_conf2}%\n\n{_reason2}")
                            if _sec2 and not _vr2.get("match"):
                                st.warning(f"يجب نقله → **{_sec2}**")

            with b9:  # تاريخ السعر
                if st.button("📈 تاريخ", key=f"ph_{prefix}_{idx}"):
                    history = get_price_history(our_name, comp_src)
                    if history:
                        rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                        st.info("\n".join(rows_h))
                    else:
                        st.info("لا يوجد تاريخ بعد")

        _hr_m = "3px 0" if (compact_cards and prefix == "raise") else "6px 0"
        st.markdown(
            f'<hr style="border:none;border-top:1px solid #1a1a2e;margin:{_hr_m}">',
            unsafe_allow_html=True,
        )


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — أي مزود (Gemini و/أو OpenRouter و/أو Cohere) يكفي للمسار الهجين
    ai_ok = ANY_AI_PROVIDER_CONFIGURED
    if ai_ok:
        ai_color = "#00C853"
        _ai_bits = []
        if GEMINI_API_KEYS:
            _ai_bits.append(f"Gemini×{len(GEMINI_API_KEYS)}")
        if (OPENROUTER_API_KEY or "").strip():
            _ai_bits.append("OpenRouter")
        if (COHERE_API_KEY or "").strip():
            _ai_bits.append("Cohere")
        ai_label = f"🤖 {' · '.join(_ai_bits)} ✅"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — أضف مفتاحاً (Gemini أو OpenRouter أو Cohere)"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )

    # زر تشخيص سريع — Railway يستخدم متغيرات البيئة وليس secrets.toml
    if not ai_ok:
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            import os

            def _mask(v: str) -> str:
                v = str(v or "").strip()
                if len(v) <= 12:
                    return "***" if v else ""
                return v[:8] + "…" + v[-4:]

            st.info(
                "على **Railway / Docker**: أضف **أحد** المسارات: `GEMINI_API_KEY` / `GEMINI_API_KEYS` "
                "أو **`OPENROUTER_API_KEY`** أو **`COHERE_API_KEY`** في Variables للخدمة "
                "(لا يعتمد التطبيق على ملف secrets.toml هناك). المحرك يجرّب Gemini ثم OpenRouter ثم Cohere."
            )
            st.write("**متغيرات البيئة — Gemini:**")
            _any = False
            for key_name in (
                "GEMINI_API_KEYS",
                "GEMINI_API_KEY",
                "GEMINI_KEY_1",
                "GEMINI_KEY_2",
                "GEMINI_KEY_3",
            ):
                raw = os.environ.get(key_name, "")
                if raw:
                    _any = True
                    st.success(f"✅ `{key_name}` = `{_mask(raw)}` (طول {len(raw)})")
                else:
                    st.caption(f"— `{key_name}` غير مضبوط")
            st.write("**متغيرات البيئة — بدائل (كافية بدون Gemini):**")
            for key_name in ("OPENROUTER_API_KEY", "OPENROUTER_KEY", "COHERE_API_KEY"):
                raw = os.environ.get(key_name, "")
                if raw:
                    _any = True
                    st.success(f"✅ `{key_name}` = `{_mask(raw)}` (طول {len(raw)})")
                else:
                    st.caption(f"— `{key_name}` غير مضبوط")
            st.write(
                f"**ما يقرأه التطبيق:** Gemini={len(GEMINI_API_KEYS)} | "
                f"OpenRouter={'نعم' if (OPENROUTER_API_KEY or '').strip() else 'لا'} | "
                f"Cohere={'نعم' if (COHERE_API_KEY or '').strip() else 'لا'}"
            )
            if not _any:
                st.warning(
                    "لم يُعثر على أي مفتاح. إما مفتاح **Google AI Studio** (`GEMINI_API_KEY`) "
                    "أو مفتاح **OpenRouter** (`OPENROUTER_API_KEY`) — الأخير يكفي لتشغيل مسار الـ fallback."
                )
            st.write("**Streamlit secrets (اختياري — Streamlit Cloud فقط):**")
            try:
                _sk = list(st.secrets.keys())
                for k in _sk:
                    val = str(st.secrets[k])
                    st.caption(f"  `{k}` = `{_mask(val)}`")
                if not _sk:
                    st.caption("لا مفاتيح — طبيعي على Railway عند الاعتماد على Variables فقط.")
            except Exception as e:
                st.caption(f"لا ملف secrets (طبيعي على Railway): {e}")

    # حالة المعالجة — تحديث حي مع auto-rerun
    if st.session_state.job_id:
        job = get_job_progress(st.session_state.job_id)
        if job:
            if job["status"] == "running":
                tot = max(int(job.get("total") or 0), 1)
                proc = min(int(job.get("processed") or 0), tot)
                pct = proc / tot
                pct_lbl = f"{100.0 * pct:.1f}%"
                st.progress(
                    min(pct, 0.99),
                    f"⚙️ {proc}/{tot} منتج — {pct_lbl}",
                )
                st.caption("تحليل خلفي — يُحدَّث كل بضع ثوانٍ. لا تغلق الصفحة حتى يكتمل.")
                # تحديث تلقائي كل 4 ثوانٍ بدون إعادة تشغيل الكود كاملاً
                try:
                    from streamlit_autorefresh import st_autorefresh
                    st_autorefresh(interval=4000, key="progress_refresh")
                except ImportError:
                    # fallback: rerun عادي إذا لم تكن المكتبة موجودة
                    time.sleep(4)
                    st.rerun()
            elif job["status"] == "done" and st.session_state.job_running:
                # اكتمل — حمّل النتائج تلقائياً مع استعادة القوائم
                if job.get("results"):
                    _restored = restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _prev_all_bg = (
                        st.session_state.results.get("all", pd.DataFrame())
                        if st.session_state.results else pd.DataFrame()
                    )
                    df_all = _merge_analysis_results(df_all, _prev_all_bg)
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                st.session_state.last_audit_stats = job.get("audit") or {}
                st.session_state.job_running = False
                # علّم البيانات بأنها من الجلسة الحالية
                st.session_state["results_from_session"] = True
                st.session_state["results_loaded_at"]    = job.get("updated_at", "")
                st.balloons()
                st.rerun()
            elif job["status"].startswith("error"):
                st.error(f"❌ فشل: {job['status'][7:80]}")
                st.session_state.job_running = False

    page = st.radio("الأقسام", SECTIONS, label_visibility="collapsed", key="main_nav")

    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        # شارة تُوضّح مصدر البيانات المعروضة
        if st.session_state.get("results_from_session"):
            st.markdown(
                '<div style="background:#0a2a0a;border:1px solid #00C853;border-radius:6px;'
                'padding:4px 8px;font-size:.72rem;margin-bottom:4px">'
                '🟢 <b>بيانات الجلسة الحالية</b></div>',
                unsafe_allow_html=True,
            )
        else:
            _loaded_at = st.session_state.get("results_loaded_at", "")
            _ts_label  = str(_loaded_at)[:16] if _loaded_at else "تشغيل سابق"
            st.markdown(
                f'<div style="background:#1a1a1a;border:1px dashed #888;border-radius:6px;'
                f'padding:4px 8px;font-size:.72rem;margin-bottom:4px">'
                f'🕐 <b>بيانات محملة من:</b> {_ts_label}<br>'
                f'<span style="color:#aaa">ارفع ملفات جديدة للتحليل الحالي</span></div>',
                unsafe_allow_html=True,
            )
        st.markdown("**📊 ملخص:**")
        for key, icon, label in [
            ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
            ("approved","✅","موافق"), ("missing","🔍","مفقود"),
            ("review","⚠️","مراجعة"), ("excluded","⚪","مستبعد"),
        ]:
            cnt = len(r.get(key, pd.DataFrame()))
            st.caption(f"{icon} {label}: **{cnt}**")
        # ملخص الثقة للمفقودات
        _miss_df = r.get("missing", pd.DataFrame())
        if not _miss_df.empty and "مستوى_الثقة" in _miss_df.columns:
            _gc = len(_miss_df[_miss_df["مستوى_الثقة"] == "green"])
            _yc = len(_miss_df[_miss_df["مستوى_الثقة"] == "yellow"])
            _rc = len(_miss_df[_miss_df["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="background:#1a1a2e;border-radius:6px;padding:6px;margin-top:4px;font-size:.75rem">'
                f'🟢 مؤكد: <b>{_gc}</b> &nbsp; '
                f'🟡 محتمل: <b>{_yc}</b> &nbsp; '
                f'🔴 مشكوك: <b>{_rc}</b></div>',
                unsafe_allow_html=True)

    # قرارات معلقة
    pending_cnt = len(st.session_state.decisions_pending)
    if pending_cnt:
        st.markdown(f'<div style="background:#FF174422;border:1px solid #FF1744;'
                    f'border-radius:6px;padding:6px;text-align:center;color:#FF1744;'
                    f'font-size:.8rem">📦 {pending_cnt} قرار معلق</div>',
                    unsafe_allow_html=True)

    # ── فلاتر سريعة عالمية في نهاية الشريط الجانبي ──
    if st.session_state.results:
        _all_df = st.session_state.results.get("all", pd.DataFrame())
        if not _all_df.empty:
            render_sidebar_filters(_all_df)

    # ── تحذيرات الفحص الذاتي — في الشريط الجانبي فقط ───────────────────
    _hs_sb = st.session_state.get("health_status", {})
    _sb_warns = _hs_sb.get("warnings", [])
    if _sb_warns:
        st.sidebar.markdown("---")
        for _w in _sb_warns:
            st.sidebar.caption(f"🔔 {_w}")


# إشعار خفيف بعد الانتقال من أزرار لوحة التحكم
if st.session_state.get("nav_flash"):
    _nf = st.session_state.pop("nav_flash", None)
    if _nf:
        if hasattr(st, "toast"):
            st.toast(_nf, icon="⏳")
        else:
            st.info(_nf)

# Toast نتائج Callbacks (إرسال Make / فشل)
_at = st.session_state.pop("_action_toast", None)
if _at:
    _at_type, _at_msg = _at
    if hasattr(st, "toast"):
        st.toast(_at_msg, icon="✅" if _at_type == "success" else "❌")
    elif _at_type == "success":
        st.success(_at_msg)
    else:
        st.error(_at_msg)


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    db_log("dashboard", "view")
    if st.session_state.get("last_audit_stats"):
        _render_audit_bar(st.session_state.last_audit_stats)

    # تغييرات الأسعار
    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(c_df[["product_name","competitor","old_price","new_price",
                            "price_diff","new_date"]].rename(columns={
            "product_name": "المنتج", "competitor": "المنافس",
            "old_price": "السعر السابق", "new_price": "السعر الجديد",
            "price_diff": "التغيير", "new_date": "التاريخ"
        }).head(200), use_container_width=True, height=200)
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        _dash_nav = [
            ("🔴 سعر أعلى", "🔴", "سعر أعلى", "price_raise"),
            ("🟢 سعر أقل", "🟢", "سعر أقل", "price_lower"),
            ("✅ موافق عليها", "✅", "موافق", "approved"),
            ("🔍 منتجات مفقودة", "🔍", "مفقود", "missing"),
            ("⚠️ تحت المراجعة", "⚠️", "مراجعة", "review"),
            ("⚪ مستبعد (لا يوجد تطابق)", "⚪", "مستبعد", "excluded"),
        ]
        cols = st.columns(6)
        for col, (sec_title, icon, short_lbl, rkey) in zip(cols, _dash_nav):
            val = len(r.get(rkey, pd.DataFrame()))
            with col:
                if st.button(
                    f"{icon} {val}\n{short_lbl}",
                    key=f"dash_go_{rkey}",
                    width='stretch',
                    help=f"انتقل إلى {sec_title}",
                ):
                    st.session_state._nav_pending = sec_title
                    st.session_state.nav_flash = f"➡️ {sec_title}"
                    st.rerun()

        # ملخص الثقة للمفقودات في لوحة التحكم
        _miss_dash = r.get("missing", pd.DataFrame())
        if not _miss_dash.empty and "مستوى_الثقة" in _miss_dash.columns:
            _g = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "green"])
            _y = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "yellow"])
            _rd = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="display:flex;gap:12px;justify-content:center;padding:8px;'
                f'background:#1a1a2e;border-radius:8px;margin:8px 0">'
                f'<span style="color:#00C853">🟢 مؤكد: <b>{_g}</b></span>'
                f'<span style="color:#FFD600">🟡 محتمل: <b>{_y}</b></span>'
                f'<span style="color:#FF1744">🔴 مشكوك: <b>{_rd}</b></span>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [("price_raise","سعر_أعلى"),("price_lower","سعر_أقل"),
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة"),
                               ("excluded","مستبعد")]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button("📥 تصدير كل الأقسام Excel",
                    data=excel_all, file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with cc2:
            if st.button("📤 إرسال كل شيء لـ Make (دفعات ذكية)",
                         key="dash_send_all_make"):
                _prog_all = st.progress(0, text="جاري الإرسال...")
                _status_all = st.empty()
                _sent_total = 0
                _fail_total = 0
                _sections = [
                    ("price_raise", "raise", "update", "🔴 سعر أعلى"),
                    ("price_lower", "lower", "update", "🟢 سعر أقل"),
                    ("approved",    "approved", "update", "✅ موافق"),
                    ("missing",     "missing", "new", "🔍 مفقودة"),
                ]
                for _si, (_key, _sec, _btype, _label) in enumerate(_sections):
                    if _key in r and not r[_key].empty:
                        _p = export_to_make_format(r[_key], _sec)
                        _res = send_batch_smart(_p, batch_type=_btype, batch_size=20, max_retries=3)
                        _sent_total += _res.get("sent", 0)
                        _fail_total += _res.get("failed", 0)
                        _status_all.caption(f"{_label}: ✅ {_res.get('sent',0)} | ❌ {_res.get('failed',0)}")
                    _prog_all.progress((_si + 1) / len(_sections), text=f"جاري: {_label}")
                _prog_all.progress(1.0, text="اكتمل")
                st.success(f"✅ تم إرسال {_sent_total} منتج لـ Make!" + (f" (فشل {_fail_total})" if _fail_total else ""))
    else:
        # استئناف آخر job؟
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results"):
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                _restored_last = restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                if not df_all.empty:
                    missing_df = pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    st.rerun()
        else:
            st.info("👈 ارفع الملفات في القسم أدناه ثم اضغط «بدء التحليل»")

    st.markdown("---")
    st.subheader("📂 رفع الملفات وبدء التحليل")

    our_file = st.file_uploader(
        "📦 ملف منتجاتنا (CSV/Excel) — اختياري إذا تم الرفع مسبقاً",
        type=["csv", "xlsx", "xls"],
        key="dash_our_file",
    )

    # ── منطق Local Persistence لكتالوج المتجر ────────────────────────────
    our_df = None
    _our_file_name = "saved_our_catalog.csv"

    if our_file is not None:
        _our_df_raw, _our_err = read_file(our_file)
        try:
            our_file.seek(0)
        except Exception:
            pass
        if _our_err:
            st.error(f"❌ {_our_err}")
        else:
            our_df = _our_df_raw
            _our_file_name = our_file.name
            os.makedirs("data", exist_ok=True)
            our_df.to_csv(OUR_CATALOG_PATH, index=False, encoding="utf-8-sig")
            st.success(f"✅ تم تحديث الكتالوج الأساسي وحفظه بنجاح! ({len(our_df):,} منتج)")
    elif os.path.exists(OUR_CATALOG_PATH):
        try:
            our_df = pd.read_csv(OUR_CATALOG_PATH, encoding="utf-8-sig")
            st.info(f"💾 يتم استخدام الكتالوج المحفوظ مسبقاً ({len(our_df):,} منتج) — ارفع ملفاً جديداً لتحديثه.")
        except Exception as _load_err:
            st.warning(f"⚠️ تعذّر تحميل الكتالوج المحفوظ: {_load_err}")
    else:
        st.warning("⚠️ لم يتم العثور على كتالوج محفوظ. يرجى رفع ملف متجرك لأول مرة.")

    # ── جسر الكشط التلقائي (Auto-Scraper Bridge) ─────────────────────────
    import os as _os_dash
    _AUTO_CSV = _os_dash.path.join(
        _os_dash.environ.get("DATA_DIR", "data"), "competitors_latest.csv"
    )
    _auto_available = _os_dash.path.exists(_AUTO_CSV)
    _auto_rows = 0   # ← يُهيَّأ دائماً لمنع NameError إذا تغيّرت حالة الملف بين reruns

    if _auto_available:
        _auto_rows = 0
        try:
            with open(_AUTO_CSV, encoding="utf-8-sig") as _af:
                _auto_rows = sum(1 for _ in _af) - 1
        except Exception:
            pass
        st.markdown(
            f'<div style="background:#0a2a0a;border:1px solid #00C853;border-radius:8px;'
            f'padding:10px 14px;margin:6px 0;font-size:.88rem">'
            f'🤖 <b>بيانات الكشط التلقائي جاهزة</b> — '
            f'{_auto_rows:,} منتج من المنافسين<br>'
            f'<span style="color:#9e9e9e;font-size:.78rem">'
            f'استخدمها مباشرةً بدلاً من رفع ملف يدوي</span></div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="background:#1a1a1a;border:1px dashed #555;border-radius:8px;'
            'padding:8px 14px;margin:6px 0;font-size:.82rem;color:#888">'
            '🤖 البيانات التلقائية غير متوفرة بعد — '
            '<a href="#" style="color:#4fc3f7">اذهب لصفحة الكشط</a> لتشغيل المحرك</div>',
            unsafe_allow_html=True,
        )

    _use_auto = st.checkbox(
        "🤖 استخدام بيانات الكشط التلقائي من المنافسين",
        value=bool(st.session_state.pop("_use_auto_scraper", False)) and _auto_available,
        disabled=not _auto_available,
        key="dash_use_auto_scraper",
        help="يستخدم الملف المُنتج تلقائياً من محرك الكشط بدلاً من رفع ملف يدوياً",
    )

    if not _use_auto:
        # ── حالة الكتالوج المتراكم للمنافسين ────────────────────────────
        _master_path = get_master_competitors_path()
        _master_rows = 0
        if os.path.exists(_master_path):
            try:
                with open(_master_path, encoding="utf-8-sig") as _mf:
                    _master_rows = sum(1 for _ in _mf) - 1
            except Exception:
                pass
            st.markdown(
                f'<div style="background:#0a1a2a;border:1px solid #1565C0;border-radius:8px;'
                f'padding:10px 14px;margin:6px 0;font-size:.88rem">'
                f'🗄️ <b>كتالوج المنافسين المتراكم جاهز</b> — '
                f'{_master_rows:,} منتج محفوظ من جلسات سابقة<br>'
                f'<span style="color:#9e9e9e;font-size:.78rem">'
                f'ارفع ملفات جديدة لإضافة منافسين أو تحديث الأسعار، أو اضغط "بدء التحليل" مباشرةً</span></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div style="background:#1a1a1a;border:1px dashed #555;border-radius:8px;'
                'padding:8px 14px;margin:6px 0;font-size:.82rem;color:#888">'
                '🗄️ لا يوجد كتالوج منافسين محفوظ بعد — ارفع ملف منافس واحد على الأقل للبدء</div>',
                unsafe_allow_html=True,
            )

        comp_files = st.file_uploader(
            "🏪 ملفات المنافسين — اختياري لتحديث/إضافة منافسين جدد",
            type=["csv", "xlsx", "xls"],
            accept_multiple_files=True,
            key="dash_comp_files",
        )
    else:
        comp_files = None  # غير مستخدم عند التحميل التلقائي
        _master_path = None
        _master_rows = 0
        st.success(
            f"✅ سيُستخدم الملف الآلي: `{_AUTO_CSV}` ({_auto_rows:,} منتج)"
        )

    if our_df is not None:
        with st.expander("📋 تعرف تلقائي على أعمدة ملف المتجر", expanded=False):
            _render_column_mapping_expander(our_df, "dash_map_our")
    if comp_files:
        for _ci, cf in enumerate(comp_files):
            try:
                cf.seek(0)
            except Exception:
                pass
            _cdf, _ce = read_file(cf)
            try:
                cf.seek(0)
            except Exception:
                pass
            if not _ce and _cdf is not None:
                with st.expander(f"📋 تعرف تلقائي — {cf.name}", expanded=False):
                    _render_column_mapping_expander(_cdf, f"dash_map_comp_{_ci}")

    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        bg_mode = st.checkbox(
            "⚡ معالجة خلفية (يمكنك التنقل أثناء التحليل)",
            value=True,
            key="dash_bg_mode",
        )
    with col_opt2:
        max_rows = st.number_input(
            "حد الصفوف للمعالجة (0=كل)", 0, step=500, key="dash_max_rows"
        )

    if st.button("🚀 بدء التحليل", type="primary", key="dash_btn_start_analysis"):
        # ── حارس المدخلات (يدعم الوضعين: يدوي وتلقائي) ──────────────────
        _auto_mode = bool(st.session_state.get("dash_use_auto_scraper")) and _auto_available
        _master_path_btn = get_master_competitors_path()
        _master_exists = os.path.exists(_master_path_btn)
        if our_df is None or (hasattr(our_df, "empty") and our_df.empty):
            st.error("⚠️ يرجى رفع ملف المتجر لأول مرة للبدء.")
        elif not _auto_mode and not comp_files and not _master_exists:
            st.warning("⚠️ ارفع ملف منافس واحد على الأقل — لا يوجد كتالوج منافسين محفوظ بعد")
        else:
            _prep_ok = False
            comp_dfs = {}
            job_id = None
            comp_names = ""
            with st.spinner("⏳ جاري قراءة الملفات وتحديث الكتالوج..."):
                # our_df قد يكون محمّلاً مسبقاً (من ملف مرفوع جديد أو من القرص)
                if our_file is not None:
                    _fresh_df, err = read_file(our_file)
                    try:
                        our_file.seek(0)
                    except Exception:
                        pass
                    if err:
                        st.error(f"❌ {err}")
                        our_df = None
                    else:
                        our_df = _fresh_df
                else:
                    err = None  # our_df محمّل من القرص مسبقاً
                if our_df is not None and not (hasattr(our_df, "empty") and our_df.empty):
                    our_df = apply_user_column_map(our_df, **_effective_column_map(our_df, "dash_map_our"))
                    if max_rows > 0:
                        our_df = our_df.head(int(max_rows))
                if our_df is None or (hasattr(our_df, "empty") and our_df.empty):
                    err = "تعذّر تحميل بيانات المتجر"
                if err is None or err == "":
                    err = False  # لمنع دخول شرط الخطأ أدناه

                    comp_dfs = {}
                    if _auto_mode:
                        # ── وضع الكشط التلقائي: تحميل CSV من القرص ────────
                        try:
                            _auto_df = pd.read_csv(_AUTO_CSV, encoding="utf-8-sig")

                            # تحويل الأسعار من USD إلى SAR إذا كانت الأسعار بالدولار
                            # (يحدث عندما يكشط السيرفر قبل تطبيق إصلاح العملة)
                            if "price" in _auto_df.columns:
                                _prices_num = pd.to_numeric(_auto_df["price"], errors="coerce").dropna()
                                if len(_prices_num) > 0:
                                    _avg_p = _prices_num.mean()
                                    _max_p = _prices_num.max()
                                    # أسعار الريال السعودي للعطور عادةً 50-5000 ر.س
                                    # أسعار الدولار عادةً أقل بـ 3.75 مرة
                                    # إذا كان المتوسط < 300 والأقصى < 1200 → الأرجح دولار
                                    if _avg_p < 300 and _max_p < 1500:
                                        _auto_df = _auto_df.copy()
                                        _auto_df["price"] = pd.to_numeric(
                                            _auto_df["price"], errors="coerce"
                                        ).fillna(0) * 3.75
                                        st.caption("💱 تم تحويل الأسعار: USD → SAR (×3.75)")

                            # تقسيم بيانات الكاشط حسب المتجر لإظهار اسم المنافس الصحيح
                            if "store" in _auto_df.columns:
                                _store_counts = []
                                for _store_name, _store_df in _auto_df.groupby("store", sort=False):
                                    _sname = str(_store_name).strip() or "competitors_latest.csv"
                                    comp_dfs[_sname] = _store_df.reset_index(drop=True)
                                    _store_counts.append(f"{_sname}({len(_store_df)})")
                                st.caption(f"✅ تم تحميل البيانات الآلية: {len(_auto_df):,} منتج — {', '.join(_store_counts)}")
                            else:
                                comp_dfs["competitors_latest.csv"] = _auto_df
                            st.caption(f"✅ تم تحميل البيانات الآلية: {len(_auto_df):,} منتج")
                        except Exception as _ae:
                            st.error(f"❌ فشل تحميل الملف الآلي: {_ae}")
                    else:
                        # ── وضع الرفع اليدوي + الذاكرة التراكمية ─────────
                        _new_comp_dfs = {}
                        for _ci, cf in enumerate(comp_files or []):
                            cdf, cerr = read_file(cf)
                            if cerr:
                                st.warning(f"⚠️ {cf.name}: {cerr}")
                            else:
                                cdf = apply_user_column_map(
                                    cdf, **_effective_column_map(cdf, f"dash_map_comp_{_ci}")
                                )
                                _new_comp_dfs[cf.name] = cdf

                        # دمج الجديد مع الكتالوج المتراكم على القرص
                        try:
                            _upsert_result = upsert_competitors(_new_comp_dfs)
                            comp_dfs, _master_total, _deduped = _upsert_result
                            if _new_comp_dfs:
                                st.caption(
                                    f"🗄️ تم تحديث الكتالوج المتراكم: "
                                    f"{_master_total:,} منتج إجمالي "
                                    f"({_deduped:,} تكرار حُذف)"
                                )
                            else:
                                st.caption(
                                    f"🗄️ يتم استخدام الكتالوج المتراكم المحفوظ: "
                                    f"{_master_total:,} منتج من جلسات سابقة"
                                )
                        except Exception as _ue:
                            st.warning(f"⚠️ تعذّر تحديث الكتالوج المتراكم: {_ue}")
                            comp_dfs = _new_comp_dfs  # احتياطي: الملفات الجديدة فقط

                    if not comp_dfs:
                        st.error("❌ لم يُحمّل أي ملف منافس صالح")
                    else:
                        _catc = resolve_catalog_columns(our_df)
                        r_our = upsert_our_catalog(
                            our_df,
                            name_col=_catc["name"] or "اسم المنتج",
                            id_col=_catc["id"] or "رقم المنتج",
                            price_col=_catc["price"] or "سعر المنتج",
                        )
                        r_comp = upsert_comp_catalog(comp_dfs)
                        st.caption(
                            f"✅ كتالوجنا: {r_our['inserted']} جديد / {r_our['updated']} تحديث | "
                            f"المنافسين: {r_comp['new_products']} جديد / {r_comp.get('updated', 0)} تحديث"
                        )
                        st.session_state.our_df = our_df
                        st.session_state.comp_dfs = comp_dfs
                        job_id = str(uuid.uuid4())[:8]
                        st.session_state.job_id = job_id
                        comp_names = ",".join(comp_dfs.keys())
                        _prep_ok = True

            if _prep_ok and our_df is not None and comp_dfs:
                _validate_uploaded_catalog(our_df, "ملف منتجاتنا")
                for _cfn, _cdf in comp_dfs.items():
                    _validate_uploaded_catalog(_cdf, f"ملف منافس: {_cfn}")
                if bg_mode:
                    t = threading.Thread(
                        target=_run_analysis_background,
                        args=(job_id, our_df, comp_dfs, _our_file_name, comp_names),
                        daemon=True,
                    )
                    add_script_run_ctx(t)
                    t.start()
                    st.session_state.job_running = True
                    st.session_state["results_from_session"] = True
                    st.session_state["results_loaded_at"]    = ""
                    st.success(f"✅ بدأ التحليل في الخلفية (Job: {job_id})")
                    st.rerun()
                else:
                    prog = st.progress(0, "جاري التحليل...")

                    def upd(p, _r=None):
                        prog.progress(min(float(p), 0.99), f"{float(p)*100:.0f}%")

                    df_all, audit_stats = run_full_analysis(our_df, comp_dfs, progress_callback=upd)
                    st.session_state.last_audit_stats = audit_stats
                    _render_audit_bar(audit_stats)
                    raw_missing_df = find_missing_products(our_df, comp_dfs)
                    missing_df = smart_missing_barrier(raw_missing_df, our_df)

                    for _, row in df_all.iterrows():
                        if row.get("نسبة_التطابق", 0) > 0:
                            upsert_price_history(
                                str(row.get("المنتج", "")),
                                str(row.get("المنافس", "")),
                                safe_float(row.get("سعر_المنافس", 0)),
                                safe_float(row.get("السعر", 0)),
                                safe_float(row.get("الفرق", 0)),
                                safe_float(row.get("نسبة_التطابق", 0)),
                                str(row.get("القرار", "")),
                            )

                    # دمج مع النتائج السابقة إن وجدت (يمنع حذف مطابقات من مصادر سابقة)
                    _prev_all = (
                        st.session_state.results.get("all", pd.DataFrame())
                        if st.session_state.results else pd.DataFrame()
                    )
                    df_all = _merge_analysis_results(df_all, _prev_all)

                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results = _r
                    st.session_state.analysis_df = df_all
                    log_analysis(
                        _our_file_name,
                        comp_names,
                        len(our_df),
                        int((df_all.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
                        len(missing_df),
                    )
                    prog.progress(1.0, "✅ اكتمل!")
                    st.balloons()
                    st.rerun()


# ════════════════════════════════════════════════
#  2. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    st.markdown(
        '<div style="display:flex;flex-wrap:wrap;align-items:center;gap:10px;margin:0 0 4px 0">'
        '<span class="b-high" style="display:inline-block;padding:6px 12px;border-radius:10px;'
        'font-weight:800;font-size:.95rem">🔴 فرصة خفض</span>'
        '<span style="color:#9e9e9e;font-size:.82rem;font-weight:600">مقارنة مع أقل سعر منافس</span>'
        "</div>",
        unsafe_allow_html=True,
    )
    st.header("منتجات سعرنا أعلى")
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        if not df.empty:
            st.markdown(
                f'<p style="margin:4px 0 8px;font-size:1.05rem;font-weight:700;color:#FF5252">'
                f"{len(df)} منتج — سعرنا أعلى من المنافس (بيانات التحليل الحالي)</p>",
                unsafe_allow_html=True,
            )
            # AI تدريب لهذا القسم
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 احصل على تحليل شامل للقسم", key="ai_section_raise"):
                    with st.spinner("🤖 AI يحلل البيانات الفعلية..."):
                        _top = df.nlargest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق +{safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _avg_diff = safe_float(df["الفرق"].mean()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أعلى من المنافسين.\n"
                                   f"متوسط الفرق: {_avg_diff:.0f} ر.س\n"
                                   f"أعلى 15 فرق:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يجب خفض سعرها فوراً (فرق>30)؟\n"
                                   f"2. أي المنتجات يمكن إبقاؤها (فرق<10)؟\n"
                                   f"3. استراتيجية تسعير مخصصة لكل ماركة")
                        r = call_ai(_prompt, "price_raise")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "raise", "raise", compact_cards=True)
        else:
            st.success("✅ ممتاز! لا توجد منتجات بسعر أعلى")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات سعرنا أقل — فرصة رفع")
    db_log("price_lower", "view")
    if st.session_state.results and "price_lower" in st.session_state.results:
        df = st.session_state.results["price_lower"]
        if not df.empty:
            st.info(f"💰 {len(df)} منتج يمكن رفع سعره لزيادة الهامش")
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 استراتيجية رفع الأسعار", key="ai_section_lower"):
                    with st.spinner("🤖 AI يحلل فرص الربح..."):
                        _top = df.nsmallest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق {safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _total_lost = safe_float(df["الفرق"].sum()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أقل من المنافسين.\n"
                                   f"إجمالي الأرباح الضائعة: {abs(_total_lost):.0f} ر.س\n"
                                   f"أكبر 15 فرصة ربح:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يمكن رفع سعرها فوراً (فرق>50)؟\n"
                                   f"2. أي المنتجات نرفعها تدريجياً (فرق 10-50)؟\n"
                                   f"3. كم الربح المتوقع إذا رفعنا الأسعار؟")
                        r = call_ai(_prompt, "price_lower")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "lower", "lower")
        else:
            st.info("لا توجد منتجات")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.header("✅ منتجات موافق عليها")
    db_log("approved", "view")
    if st.session_state.results and "approved" in st.session_state.results:
        df = st.session_state.results["approved"]
        if not df.empty:
            st.success(f"✅ {len(df)} منتج بأسعار تنافسية مناسبة")
            render_pro_table(df, "approved", "approved")
        else:
            st.info("لا توجد منتجات موافق عليها")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  6. منتجات مفقودة — v26 مع كشف التستر/الأساسي
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات مفقودة من كتالوجنا")
    db_log("missing", "view")
    # ════════════════════════════ نقطة الدخول ════════════════════════════
    if not (st.session_state.results and "missing" in st.session_state.results):
        st.info("ارفع الملفات أولاً")
    else:
        df = st.session_state.results["missing"]
        if df is None or df.empty:
            st.success("✅ لا توجد منتجات مفقودة في التحليل الحالي!")
        else:
            # ── إحصاءات ──────────────────────────────────────────────────
            total_missing     = len(df)
            confirmed_missing = (
                len(df[df["حالة_المنتج"].str.contains("مفقود مؤكد", na=False)])
                if "حالة_المنتج" in df.columns else total_missing
            )
            potential_dups  = (
                len(df[df["حالة_المنتج"].str.contains("مكرر محتمل", na=False)])
                if "حالة_المنتج" in df.columns else 0
            )
            variants_count  = (
                len(df[df["نوع_متاح"].str.strip() != ""])
                if "نوع_متاح" in df.columns else 0
            )
            _mc1, _mc2, _mc3, _mc4 = st.columns(4)
            _mc1.metric("📦 الإجمالي",       total_missing)
            _mc2.metric("✅ مفقود مؤكد",     confirmed_missing)
            _mc3.metric("⚠️ مكرر محتمل",    potential_dups)
            _mc4.metric("🏷️ نسخة متوفرة",  variants_count)
            if "مستوى_الثقة" in df.columns:
                _gc = len(df[df["مستوى_الثقة"] == "green"])
                _yc = len(df[df["مستوى_الثقة"] == "yellow"])
                _rc = len(df[df["مستوى_الثقة"] == "red"])
                st.markdown(
                    f'<div style="background:#0d1a2e;border-radius:6px;padding:5px 10px;'
                    f'margin:4px 0 8px;font-size:.75rem;color:#aaa">'
                    f'🟢 <b style="color:#4caf50">{_gc}</b> مؤكد &nbsp;'
                    f'🟡 <b style="color:#ff9800">{_yc}</b> محتمل &nbsp;'
                    f'🔴 <b style="color:#f44336">{_rc}</b> مشكوك</div>',
                    unsafe_allow_html=True,
                )

            # ── فلاتر ──────────────────────────────────────────────────────
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر البحث", expanded=False):
                _fc1, _fc2, _fc3 = st.columns(3)
                _fc4, _fc5, _fc6 = st.columns(3)
                _fc1.text_input("🔎 بحث نصي",   key="miss_s")
                _fc2.selectbox("الماركة",        opts["brands"],      key="miss_b")
                _fc3.selectbox("المنافس",        opts["competitors"], key="miss_c")
                _fc4.selectbox("النوع",          ["الكل", "مفقود فعلاً", "يوجد تستر", "يوجد الأساسي"], key="miss_v")
                _fc5.selectbox("الثقة",          ["الكل", "🟢 مؤكد", "🟡 محتمل", "🔴 مشكوك"],          key="miss_conf_f")
                _fc6.selectbox("حالة المنتج",    ["الكل", "✅ مفقود مؤكد", "⚠️ مكرر محتمل"],          key="miss_gz_f")

            # ── تطبيق الفلاتر ─────────────────────────────────────────────
            _ms_search  = st.session_state.get("miss_s",      "")
            _ms_brand   = st.session_state.get("miss_b",      "الكل")
            _ms_comp    = st.session_state.get("miss_c",      "الكل")
            _ms_variant = st.session_state.get("miss_v",      "الكل")
            _ms_conf    = st.session_state.get("miss_conf_f", "الكل")
            _ms_gz      = st.session_state.get("miss_gz_f",   "الكل")

            filtered = df.copy()
            if _ms_search:
                filtered = filtered[filtered.apply(
                    lambda _r: _ms_search.lower() in str(_r.values).lower(), axis=1)]
            if _ms_brand != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(
                    _ms_brand, case=False, na=False, regex=False)]
            if _ms_comp != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(
                    _ms_comp, case=False, na=False, regex=False)]
            if _ms_variant == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif _ms_variant == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif _ms_variant == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            if _ms_conf != "الكل" and "مستوى_الثقة" in filtered.columns:
                _cmap = {"🟢 مؤكد": "green", "🟡 محتمل": "yellow", "🔴 مشكوك": "red"}
                _cv   = _cmap.get(_ms_conf, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]
            if _ms_gz != "الكل" and "حالة_المنتج" in filtered.columns:
                if _ms_gz == "✅ مفقود مؤكد":
                    filtered = filtered[filtered["حالة_المنتج"].str.startswith("✅", na=False)]
                elif _ms_gz == "⚠️ مكرر محتمل":
                    filtered = filtered[filtered["حالة_المنتج"].str.startswith("⚠️", na=False)]
            if "مستوى_الثقة" in filtered.columns:
                _co = {"green": 0, "yellow": 1, "red": 2}
                filtered = (filtered
                    .assign(_cs=filtered["مستوى_الثقة"].map(_co).fillna(3))
                    .sort_values("_cs").drop(columns=["_cs"]))
            filtered = filtered.reset_index(drop=True)

            # ── session_state للتحديد والصفحة ─────────────────────────────
            if "miss_sel" not in st.session_state:
                st.session_state.miss_sel = set()
            if "miss_pg2" not in st.session_state:
                st.session_state.miss_pg2 = 1

            _MISS_PAGE  = 12
            _total_f    = len(filtered)
            _total_pgs  = max(1, (_total_f + _MISS_PAGE - 1) // _MISS_PAGE)
            _cur_pg     = max(1, min(st.session_state.miss_pg2, _total_pgs))

            # ── شريط التحكم في التحديد ────────────────────────────────────
            _pg_start = (_cur_pg - 1) * _MISS_PAGE
            _pg_end   = min(_pg_start + _MISS_PAGE, _total_f)
            _n_sel    = len(st.session_state.miss_sel)

            _sbc1, _sbc2, _sbc3, _sbc4 = st.columns([2, 2, 2, 4])

            def _cb_sel_page():
                for _pi in range(_pg_start, _pg_end):
                    st.session_state.miss_sel.add(_pi)

            def _cb_sel_all():
                for _pi in range(_total_f):
                    st.session_state.miss_sel.add(_pi)

            def _cb_clr_sel():
                st.session_state.miss_sel.clear()

            _sbc1.button("✓ الصفحة", on_click=_cb_sel_page, key="miss_sel_pg",  use_container_width=True)
            _sbc2.button("✓ الكل",   on_click=_cb_sel_all,  key="miss_sel_all", use_container_width=True)
            _sbc3.button("✗ مسح",    on_click=_cb_clr_sel,  key="miss_clr",     use_container_width=True)
            _sel_c = "#4caf50" if _n_sel > 0 else "#555"
            _sbc4.markdown(
                f'<div style="padding:6px 12px;border-radius:6px;background:#0d1a2e;'
                f'border:1px solid {_sel_c}44;font-size:.83rem;color:{_sel_c};margin-top:2px">'
                f'<b>{_n_sel}</b> محدد من <b>{_total_f}</b> &nbsp;'
                f'<span style="color:#555;font-size:.72rem">صفحة {_cur_pg}/{_total_pgs}</span></div>',
                unsafe_allow_html=True,
            )

            st.markdown('<div style="height:6px"></div>', unsafe_allow_html=True)

            # ══════════════════════════════════════════════════════════════
            #  شبكة بطاقات (2 عمود × 6 صف = 12 بطاقة/صفحة)
            # ══════════════════════════════════════════════════════════════
            _page_rows = filtered.iloc[_pg_start:_pg_end]

            for _ri in range(0, len(_page_rows), 2):
                _cols = st.columns(2)
                for _ci, _col in enumerate(_cols):
                    _abs_i = _pg_start + _ri + _ci
                    if _abs_i >= _total_f:
                        break
                    row = _page_rows.iloc[_ri + _ci]
                    idx = _abs_i

                    name  = str(row.get("منتج_المنافس", ""))
                    _miss_key = f"missing_{name}_{idx}"
                    if _miss_key in st.session_state.hidden_products:
                        continue

                    price           = safe_float(row.get("سعر_المنافس", 0))
                    brand           = str(row.get("الماركة",   ""))
                    comp            = str(row.get("المنافس",   ""))
                    size            = str(row.get("الحجم",     ""))
                    ptype           = str(row.get("النوع",     ""))
                    _comp_show      = _humanize_competitor_upload(comp)
                    _title_display  = _display_name_for_missing_row(row)
                    if not _title_display:
                        _u_title = competitor_product_url_from_row(row)
                        if not str(_u_title or "").strip().lower().startswith("http") and _is_http_url_text(name):
                            _u_title = name.strip()
                        if str(_u_title or "").strip().lower().startswith("http"):
                            _ft = _cached_title_from_product_url(str(_u_title).strip())
                            if _ft:
                                _title_display = _ft
                    if _title_display:
                        nm_ai = _title_display
                    elif not _is_http_url_text(name):
                        nm_ai = name
                    else:
                        _fb = f"{brand} {size} {ptype}".strip()
                        nm_ai = _fb if _fb else (_comp_show if _comp_show != "—" else "منتج")

                    note            = str(row.get("ملاحظة", ""))
                    _miss_pid_raw   = (
                        row.get("معرف_المنافس", "") or row.get("product_id", "") or
                        row.get("رقم المنتج",   "") or row.get("رقم_المنتج", "") or
                        row.get("SKU", "")          or row.get("sku", "")         or
                        row.get("الكود", "")        or row.get("كود", "")         or
                        row.get("الباركود", "")     or ""
                    )
                    _miss_pid = ""
                    if _miss_pid_raw and str(_miss_pid_raw) not in ("", "nan", "None", "0", "NaN"):
                        try:    _miss_pid = str(int(float(str(_miss_pid_raw))))
                        except: _miss_pid = str(_miss_pid_raw).strip()

                    variant_label   = str(row.get("نوع_متاح", ""))
                    variant_product = str(row.get("منتج_متاح", ""))
                    variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                    is_tester_flag  = bool(row.get("هو_تستر", False))
                    conf_level      = str(row.get("مستوى_الثقة", "green"))
                    conf_score      = safe_float(row.get("درجة_التشابه", 0))
                    suggested_price = round(price - 1, 2) if price > 0 else 0
                    _gz_status      = str(row.get("حالة_المنتج", "")).strip()
                    _gz_similar     = str(row.get("منتج_مشابه_لدينا", "")).strip()

                    _gray_zone_html = ""
                    if _gz_status.startswith("⚠️"):
                        _sim_t = (f" ← يشبه: <b>{_gz_similar[:50]}</b>" if _gz_similar else "")
                        _gray_zone_html = (
                            f'<div style="margin-top:5px;padding:3px 8px;border-radius:5px;'
                            f'background:rgba(255,152,0,.1);border:1px solid #ff980066;'
                            f'font-size:.7rem;color:#ffb74d;font-weight:700">⚠️ مكرر محتمل{_sim_t}</div>'
                        )
                    elif _gz_status.startswith("✅"):
                        _gray_zone_html = (
                            f'<div style="margin-top:4px;padding:2px 8px;border-radius:5px;'
                            f'background:rgba(0,200,83,.07);border:1px solid #00c85344;'
                            f'font-size:.7rem;color:#69f0ae;font-weight:600">✅ مفقود مؤكد</div>'
                        )

                    _is_similar     = "⚠️" in note
                    _has_variant    = bool(variant_label and variant_label.strip())
                    _is_tester_type = "تستر" in variant_label if _has_variant else False

                    if _has_variant and _is_tester_type:
                        _border = "#ff980055"; _badge_bg = "#ff9800"
                    elif _has_variant:
                        _border = "#4caf5055"; _badge_bg = "#4caf50"
                    elif _is_similar:
                        _border = "#ff572255"; _badge_bg = "#ff5722"
                    else:
                        _border = "#007bff44"; _badge_bg = "#007bff"

                    _variant_html = ""
                    if _has_variant:
                        _variant_html = (
                            f'<div style="margin-top:4px;padding:2px 8px;border-radius:5px;'
                            f'background:{_badge_bg}22;border:1px solid {_badge_bg}66;'
                            f'font-size:.7rem;color:{_badge_bg}">'
                            f'{variant_label} ({variant_score:.0f}%) → {variant_product[:40]}</div>'
                        )
                    _tester_badge = (
                        '<span style="font-size:.65rem;padding:2px 6px;border-radius:8px;'
                        'background:#9c27b022;color:#ce93d8">🏷️ تستر</span>'
                    ) if is_tester_flag else ""

                    _miss_img = str(row.get("صورة_المنافس", "") or "").strip()
                    if not _miss_img:
                        _miss_img = _first_image_url_from_row(row) or ""
                    _miss_comp_url = competitor_product_url_from_row(row)
                    if not _miss_comp_url and _is_http_url_text(name):
                        _miss_comp_url = name.strip()
                    if not _miss_img and _miss_comp_url.startswith("http"):
                        _miss_img = _cached_thumb_from_product_url(_miss_comp_url)

                    _dup_compare_html = ""
                    if _gz_status.startswith("⚠️"):
                        _our_sim_img    = str(row.get("صورة_منتجنا_المشابه", "") or "").strip()
                        _sim_n = (_gz_similar[:45] + "…") if len(_gz_similar) > 45 else _gz_similar
                        _new_n = (str(name)[:45] + "…") if len(str(name)) > 45 else str(name)

                        def _ibox(iu, lbl, sub, ac):
                            if iu and iu.startswith("http"):
                                _t = (f'<img src="{iu}" loading="lazy" decoding="async" '
                                      f'style="width:68px;height:68px;object-fit:cover;border-radius:6px;'
                                      f'border:1px solid {ac}44;display:block;margin:0 auto 3px">')
                            else:
                                _t = (f'<div style="width:68px;height:68px;border-radius:6px;'
                                      f'border:1px dashed {ac}44;display:flex;align-items:center;'
                                      f'justify-content:center;margin:0 auto 3px;font-size:1.3rem;'
                                      f'color:{ac}55">🖼️</div>')
                            return (f'<div style="text-align:center;flex:1;min-width:0">{_t}'
                                    f'<div style="font-size:.62rem;color:{ac};font-weight:700">{lbl}</div>'
                                    f'<div style="font-size:.58rem;color:#666">{sub}</div></div>')

                        _dup_compare_html = (
                            f'<div style="margin-top:7px;padding:6px 8px;border-radius:6px;'
                            f'background:rgba(255,152,0,.05);border:1px solid #ff980020">'
                            f'<div style="font-size:.62rem;color:#777;text-align:center;margin-bottom:4px">'
                            f'قارن الصورتين</div>'
                            f'<div style="display:flex;gap:10px;justify-content:center">'
                            f'{_ibox(_miss_img, "🆕 جديد", _new_n, "#4fc3f7")}'
                            f'<div style="color:#ff980055;font-size:.9rem;align-self:center">⟷</div>'
                            f'{_ibox(_our_sim_img, "📦 لدينا", _sim_n, "#ffb74d")}'
                            f'</div></div>'
                        )

                    with _col:
                        # ── checkbox تحديد ──────────────────────────────────
                        _is_chk = idx in st.session_state.miss_sel

                        def _toggle_sel(i=idx):
                            if i in st.session_state.miss_sel:
                                st.session_state.miss_sel.discard(i)
                            else:
                                st.session_state.miss_sel.add(i)

                        st.checkbox(
                            f"تحديد — {nm_ai[:30]}",
                            value=_is_chk,
                            key=f"mchk_{idx}",
                            on_change=_toggle_sel,
                        )

                        # ── بطاقة المنتج ────────────────────────────────────
                        st.markdown(miss_card(
                            name=name, price=price, brand=brand, size=size,
                            ptype=ptype, comp=_comp_show, suggested_price=suggested_price,
                            note=note if _is_similar else "",
                            variant_html=_variant_html, tester_badge=_tester_badge,
                            border_color=_border,
                            confidence_level=conf_level, confidence_score=conf_score,
                            product_id=_miss_pid,
                            image_url=_miss_img,
                            comp_url=_miss_comp_url,
                            title_override=_title_display,
                            gray_zone_html=_gray_zone_html,
                            dup_compare_html=_dup_compare_html,
                        ), unsafe_allow_html=True)

                        # ── أزرار الإجراءات ─────────────────────────────────
                        _ba1, _ba2, _ba3 = st.columns([3, 3, 2])
                        with _ba1:
                            if st.button("✍️ خبير الوصف", key=f"expert_{idx}",
                                         type="primary", use_container_width=True):
                                with st.spinner("🤖 يكتب الوصف..."):
                                    fi_cached = st.session_state.get(f"frag_info_{idx}")
                                    if not fi_cached:
                                        fi_cached = fetch_fragrantica_info(nm_ai)
                                        st.session_state[f"frag_info_{idx}"] = fi_cached
                                    desc = generate_mahwous_description(nm_ai, suggested_price, fi_cached)
                                    desc, _seo_meta = _parse_seo_json_block(desc)
                                    st.session_state[f"desc_{idx}"] = desc
                        with _ba2:
                            _has_desc = f"desc_{idx}" in st.session_state
                            _lbl = "📤 Make + وصف" if _has_desc else "📤 إرسال Make"
                            if st.button(_lbl, key=f"mk_m_{idx}", use_container_width=True):
                                with st.spinner("📤 يُرسل لـ Make..."):
                                    _res_mk = send_new_products([{
                                        "product_id": _miss_pid, "name": nm_ai,
                                        "price": float(suggested_price), "sku": _miss_pid,
                                        "weight": 1, "cost_price": 0, "sale_price": 0,
                                        "description": st.session_state.get(f"desc_{idx}", f"عطر {nm_ai} الأصلي"),
                                        "image_url": _miss_img,
                                    }])
                                if _res_mk["success"]:
                                    st.success(_res_mk["message"])
                                    st.session_state.hidden_products.add(_miss_key)
                                    st.rerun()
                                else:
                                    st.error(_res_mk["message"])
                        with _ba3:
                            if _miss_comp_url:
                                st.link_button("🔗", _miss_comp_url, use_container_width=True)
                            else:
                                if st.button("🗑", key=f"hide_m_{idx}", use_container_width=True,
                                             help="إخفاء هذا المنتج"):
                                    st.session_state.hidden_products.add(_miss_key)
                                    st.rerun()

                        # ── محرر الوصف (يظهر بعد التوليد) ──────────────────
                        if f"desc_{idx}" in st.session_state:
                            with st.expander("📄 الوصف الكامل", expanded=True):
                                edited_desc = st.text_area(
                                    "راجع وعدّل:",
                                    value=st.session_state[f"desc_{idx}"],
                                    height=200,
                                    key=f"desc_edit_{idx}",
                                )
                                st.session_state[f"desc_{idx}"] = edited_desc
                                _wc = len(edited_desc.split())
                                st.markdown(
                                    f'<span style="color:{"#4caf50" if _wc >= 500 else "#ff9800"};'
                                    f'font-size:.72rem">📊 {_wc} كلمة</span>',
                                    unsafe_allow_html=True,
                                )

            # ── ترقيم الصفحات ─────────────────────────────────────────────
            if _total_pgs > 1:
                st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
                _pg1, _pg2, _pg3, _pg4, _pg5 = st.columns([1, 1, 3, 1, 1])
                if _pg1.button("⏮", key="miss_first", disabled=_cur_pg <= 1):
                    st.session_state.miss_pg2 = 1; st.rerun()
                if _pg2.button("◀", key="miss_prev",  disabled=_cur_pg <= 1):
                    st.session_state.miss_pg2 = _cur_pg - 1; st.rerun()
                _pg3.markdown(
                    f'<div style="text-align:center;padding:5px;font-size:.82rem;color:#aaa">'
                    f'صفحة <b style="color:#fff">{_cur_pg}</b> / <b style="color:#fff">{_total_pgs}</b>'
                    f' <span style="color:#555">({_total_f} منتج)</span></div>',
                    unsafe_allow_html=True,
                )
                if _pg4.button("▶", key="miss_next", disabled=_cur_pg >= _total_pgs):
                    st.session_state.miss_pg2 = _cur_pg + 1; st.rerun()
                if _pg5.button("⏭", key="miss_last", disabled=_cur_pg >= _total_pgs):
                    st.session_state.miss_pg2 = _total_pgs; st.rerun()

            # ══════════════════════════════════════════════════════════════
            #  شريط إجراءات التحديد (يظهر فقط عند وجود تحديد)
            # ══════════════════════════════════════════════════════════════
            _n_sel = len(st.session_state.miss_sel)
            if _n_sel > 0:
                st.markdown('<hr style="border-color:#1e3a5f;margin:14px 0">', unsafe_allow_html=True)
                st.markdown(
                    f'<div style="background:linear-gradient(135deg,#0d2137,#091825);'
                    f'border:1px solid #4caf5044;border-radius:10px;padding:12px 16px;margin-bottom:10px">'
                    f'<span style="color:#4caf50;font-size:.9rem;font-weight:700">'
                    f'✅ {_n_sel} منتج محدد — اختر إجراء:</span></div>',
                    unsafe_allow_html=True,
                )
                _sel_df = filtered.iloc[sorted(st.session_state.miss_sel)].copy()
                st.info(
                    "⚠️ ملف **سلة الشامل** سيحتوي على أوصاف نصية placeholder — "
                    "استخدم زر **توليد وصف AI** لكل منتج أولاً قبل التصدير للحصول على أوصاف احترافية.",
                    icon="ℹ️",
                )
                _ac1, _ac2, _ac3, _ac4 = st.columns(4)

                with _ac1:
                    _sel_csv = export_to_salla_shamel(_sel_df, generate_descriptions=False)
                    st.download_button(
                        f"📥 سلة الشامل ({_n_sel})",
                        data=_sel_csv,
                        file_name="selected_salla.csv",
                        mime="text/csv",
                        key="miss_sel_salla",
                        use_container_width=True,
                        type="primary",
                    )
                with _ac2:
                    st.download_button(
                        f"📥 Excel ({_n_sel})",
                        data=export_to_excel(_sel_df, "مفقودة"),
                        file_name="selected_missing.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="miss_sel_xl",
                        use_container_width=True,
                    )
                with _ac3:
                    if st.button(f"🪄 وصف AI ({_n_sel})", key="miss_sel_ai",
                                 type="primary", use_container_width=True):
                        _srows = _sel_df.copy()
                        _srows["الوصف_الآلي"] = ""
                        _prog = st.progress(0, text="جاري توليد الوصف...")
                        _tot = len(_srows)
                        for _si, (_ri, _rr) in enumerate(_srows.iterrows()):
                            _pn  = str(_rr.get("منتج_المنافس", "") or "").strip()
                            _rd  = str(_rr.get("raw_description", "") or "").strip()
                            _prog.progress(_si / _tot, text=f"وصف {_si+1}/{_tot}: {_pn[:30]}…")
                            _srows.at[_ri, "الوصف_الآلي"] = generate_salla_html_description(_pn, _rd)
                        _prog.progress(1.0, text="✅ اكتمل!")
                        with st.spinner("🔗 مطابقة سلة..."):
                            _srows = map_salla_categories(_srows)
                            _srows, _mb = validate_salla_brands(_srows)
                        st.session_state["ai_gen_result_df"]      = _srows
                        st.session_state["ai_gen_missing_brands"] = _mb
                        st.session_state.pop("brands_salla_df", None)
                        st.success(f"✅ تم توليد الوصف لـ {_tot} منتج!")

                with _ac4:
                    if st.button(f"📤 Make ({_n_sel})", key="miss_sel_make",
                                 use_container_width=True):
                        is_valid, issues = validate_export_product_dataframe(_sel_df)
                        if not is_valid:
                            st.error("❌ البيانات لا تطابق معايير سلة")
                        else:
                            _sel_prods = export_to_make_format(_sel_df, "missing")
                            _pb = st.progress(0, text="جاري الإرسال...")
                            _st_ph = st.empty()

                            def _prog_cb(s, f, t, cn):
                                _pb.progress(min((s + f) / max(t, 1), 1.0))
                                _st_ph.caption(f"✅ {s} | ❌ {f} / {t}")

                            _res = send_batch_smart(_sel_prods, "new", 20, 3, _prog_cb)
                            _pb.progress(1.0)
                            if _res["success"]:
                                st.success(_res["message"])
                                st.session_state.miss_sel.clear()
                            else:
                                st.error(_res["message"])

                # ── نتيجة توليد الوصف ────────────────────────────────────
                if st.session_state.get("ai_gen_result_df") is not None:
                    _gen_df = st.session_state["ai_gen_result_df"]
                    _miss_brands = st.session_state.get("ai_gen_missing_brands") or []
                    if _miss_brands:
                        st.warning(f"⚠️ **{len(_miss_brands)} ماركة** غير مسجلة في سلة — أضفها قبل الرفع.")
                        _mbc1, _mbc2 = st.columns(2)
                        with _mbc1:
                            st.download_button(
                                "📥 أسماء الماركات (سريع)",
                                data=pd.DataFrame({"اسم الماركة": _miss_brands}).to_csv(index=False).encode("utf-8-sig"),
                                file_name="new_brands_names.csv",
                                mime="text/csv",
                                key="ai_nb_names",
                                use_container_width=True,
                            )
                        with _mbc2:
                            if st.button("🪄 قالب سلة للماركات", key="ai_brand_tpl",
                                         use_container_width=True):
                                _brows = []
                                with st.spinner("جاري التوليد..."):
                                    for _br in _miss_brands:
                                        b_data = generate_salla_brand_info(_br)
                                        _brows.append({
                                            "اسم الماركة": b_data.get("brand_name", _br),
                                            "وصف مختصر عن الماركة": b_data.get("description", ""),
                                            "صورة شعار الماركة": "",
                                            "(إختياري) صورة البانر": "",
                                            "(Page Title) عنوان صفحة العلامة التجارية": b_data.get("seo_title", ""),
                                            "(SEO Page URL) رابط صفحة العلامة التجارية": b_data.get("seo_url", ""),
                                            "(Page Description) وصف صفحة العلامة التجارية": b_data.get("seo_desc", ""),
                                        })
                                st.session_state["brands_salla_df"] = pd.DataFrame(_brows)
                                st.success(f"✅ جُهّزت {len(_brows)} ماركة")
                        if st.session_state.get("brands_salla_df") is not None:
                            st.download_button(
                                "📥 قالب الماركات (سلة الكامل)",
                                data=st.session_state["brands_salla_df"].to_csv(index=False).encode("utf-8-sig"),
                                file_name="new_brands_to_add.csv",
                                mime="text/csv",
                                type="primary",
                                key="ai_nb_full",
                                use_container_width=True,
                            )
                    _sg = format_missing_for_salla(_gen_df)
                    if not _sg.empty:
                        st.download_button(
                            "📥 ملف سلة الكامل (مع وصف AI)",
                            data=_sg.to_csv(index=False).encode("utf-8-sig"),
                            file_name="salla_ai_descriptions.csv",
                            mime="text/csv",
                            type="primary",
                            key="ai_desc_dl",
                            use_container_width=True,
                        )

            # ── تصدير الكل (يظهر دائماً) ──────────────────────────────────
            st.markdown('<hr style="border-color:#0d1a2e;margin:18px 0">', unsafe_allow_html=True)
            with st.expander("📥 تصدير الكل", expanded=False):
                st.caption(
                    "⚠️ **سلة الشامل** يحتوي على أوصاف placeholder — "
                    "فعّل توليد AI أولاً للحصول على أوصاف جاهزة للنشر."
                )
                _exa, _exb, _exc = st.columns(3)
                with _exa:
                    st.download_button(
                        "📥 Excel (الكل)",
                        data=export_to_excel(filtered, "مفقودة"),
                        file_name="missing_all.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="miss_dl_all",
                        use_container_width=True,
                    )
                with _exb:
                    st.download_button(
                        "📥 سلة الشامل (الكل)",
                        data=export_to_salla_shamel(filtered, generate_descriptions=False),
                        file_name="mahwous_salla_shamel.csv",
                        mime="text/csv",
                        key="miss_salla_all",
                        use_container_width=True,
                    )
                with _exc:
                    _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
                    _conf_sel  = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
                    _conf_val  = _conf_opts[_conf_sel]
                    if st.button("📤 إرسال الكل لـ Make", key="miss_make_all", use_container_width=True):
                        _to_send = (
                            filtered[filtered["نوع_متاح"].str.strip() == ""]
                            if "نوع_متاح" in filtered.columns else filtered
                        )
                        is_valid, issues = validate_export_product_dataframe(_to_send)
                        if not is_valid:
                            st.error("❌ تم إيقاف الإرسال — راجع جودة البيانات")
                        else:
                            _all_ps = export_to_make_format(_to_send, "missing")
                            for _ip, _pr_r in enumerate(_all_ps):
                                if _ip < len(_to_send):
                                    _pr_r["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                            _pb3 = st.progress(0, text="جاري الإرسال...")
                            _st3 = st.empty()

                            def _prog_all(s, f, t, cn):
                                _pb3.progress(min((s + f) / max(t, 1), 1.0))
                                _st3.caption(f"✅ {s} | ❌ {f} / {t}")

                            _res3 = send_batch_smart(_all_ps, "new", 20, 3, _prog_all, _conf_val)
                            _pb3.progress(1.0)
                            if _res3["success"]:
                                st.success(_res3["message"])
                            else:
                                st.error(_res3["message"])
elif page == "⚪ مستبعد (لا يوجد تطابق)":
    st.header("⚪ منتجات مستبعدة — لا يوجد تطابق مناسب مع منافس")
    st.caption(
        "منتجاتنا التي لم يُعثر لها على مرشح منافس بدرجة كافية، أو بلا أي مرشح في الفهارس. "
        "ليس نفس قسم «مفقود» (منتج عند المنافس ولا يوجد عندنا)."
    )
    db_log("excluded", "view")
    if st.session_state.results and "excluded" in st.session_state.results:
        df = st.session_state.results["excluded"]
        if df is not None and not df.empty:
            st.info(f"⚪ {len(df)} منتج مستبعد — يمكن مراجعة الأسباب في عمود القرار والمصدر")
            render_pro_table(df, "excluded", "excluded")
        else:
            st.success("✅ لا توجد منتجات مستبعدة — كل المنتجات لها مسار مطابقة أو مراجعة")
    else:
        st.info("ارفع الملفات وأجرِ التحليل أولاً")
# ════════════════════════════════════════════════
#  7. تحت المراجعة — v26 مقارنة جنباً إلى جنب
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.header("⚠️ منتجات تحت المراجعة — مطابقة غير مؤكدة")
    db_log("review", "view")

    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if df is not None and not df.empty:
            st.warning(f"⚠️ {len(df)} منتج بمطابقة غير مؤكدة — يحتاج مراجعة بشرية أو AI")

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            _rc_limit = min(len(df), 60)   # حد أقصى 60 منتج (6 دفعات × 10)
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button(
                    f"🤖 إعادة تصنيف بالذكاء الاصطناعي ({_rc_limit} منتج)",
                    type="primary", key="reclassify_review"
                ):
                    _items_rc = []
                    for _, rr in df.head(_rc_limit).iterrows():
                        _items_rc.append({
                            "our":       str(rr.get("المنتج", "")),
                            "comp":      str(rr.get("منتج_المنافس", "")),
                            "our_price": safe_float(rr.get("السعر", 0)),
                            "comp_price":safe_float(rr.get("سعر_المنافس", 0)),
                        })
                    _n_batches = max(1, (_rc_limit + 9) // 10)
                    _rc_prog   = st.progress(0, text="🤖 AI يحلل الدفعة 1 …")
                    _rc_results_all: list = []
                    for _bi, _bstart in enumerate(range(0, len(_items_rc), 10)):
                        _rc_prog.progress(
                            int((_bi / _n_batches) * 100),
                            text=f"🤖 AI يحلل الدفعة {_bi + 1} من {_n_batches} …"
                        )
                        from engines.ai_engine import _reclassify_batch
                        _rc_results_all.extend(
                            _reclassify_batch(_items_rc[_bstart:_bstart + 10], offset=_bstart)
                        )
                    _rc_prog.progress(100, text="✅ اكتمل التحليل")

                    _adf = st.session_state.get("analysis_df")
                    if _rc_results_all and _adf is not None and not _adf.empty:
                        _new_adf, _st = _apply_reclassify_to_analysis(
                            _adf, df, _rc_results_all
                        )
                        st.session_state.analysis_df = _new_adf
                        _r2 = _split_results(_new_adf)
                        _prev_miss = (
                            st.session_state.results.get("missing")
                            if st.session_state.results else None
                        )
                        if _prev_miss is not None and not (
                            isinstance(_prev_miss, pd.DataFrame) and _prev_miss.empty
                        ):
                            _r2["missing"] = _prev_miss
                        else:
                            _r2["missing"] = pd.DataFrame()
                        st.session_state.results = _r2
                        _persist_analysis_after_reclassify(_new_adf)
                        _moved     = int(_st.get("applied", 0))
                        _sk_conf   = int(_st.get("skip_conf", 0))
                        _sk_rev    = int(_st.get("skip_review", 0))
                        _sk_idx    = int(_st.get("skip_idx", 0))
                        _sk_norow  = int(_st.get("skip_no_row", 0))
                        if _moved:
                            st.success(
                                f"✅ نُقل {_moved} منتج إلى قسمه الصحيح — "
                                f"AI حلّل {len(_rc_results_all)} من {_rc_limit}"
                            )
                        else:
                            _skip_msg = []
                            if _sk_conf:
                                _skip_msg.append(f"ثقة منخفضة: {_sk_conf}")
                            if _sk_rev:
                                _skip_msg.append(f"بقي في المراجعة: {_sk_rev}")
                            if _sk_idx or _sk_norow:
                                _skip_msg.append(f"خطأ ترقيم/مطابقة: {_sk_idx + _sk_norow}")
                            st.info(
                                "ℹ️ AI قام بالتحليل لكن لم يُحرَّك أي منتج — "
                                + ("; ".join(_skip_msg) if _skip_msg else
                                   "جميع المنتجات تحتاج مراجعة يدوية")
                            )
                        st.rerun()
                    elif _rc_results_all:
                        st.warning("لا يوجد جدول تحليل (analysis_df) — لم يُحفظ التصنيف")
                    else:
                        st.error(
                            "❌ لم يتمكن AI من إعادة التصنيف — "
                            "تحقق من مفاتيح API في صفحة الإعدادات"
                        )
            with col_r2:
                excel_rv = export_to_excel(df, "مراجعة")
                st.download_button("📥 Excel", data=excel_rv, file_name="review.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rv_dl")

            # ── فلتر بحث ──────────────────────────────────────────────────
            search_rv = st.text_input("🔎 بحث باسم المنتج أو الماركة", key="rv_search",
                                      placeholder="اكتب جزءاً من الاسم…")
            df_rv = df.copy()
            if search_rv:
                _q = search_rv.strip().lower()
                # يبحث في أعمدة الاسم فقط (أسرع وأدق)
                _search_cols = [c for c in ["المنتج", "منتج_المنافس", "الماركة"]
                                if c in df_rv.columns]
                if _search_cols:
                    _mask = df_rv[_search_cols].apply(
                        lambda col: col.astype(str).str.lower().str.contains(_q, na=False)
                    ).any(axis=1)
                    df_rv = df_rv[_mask]
                else:
                    df_rv = df_rv[df_rv.apply(
                        lambda r: _q in str(r.values).lower(), axis=1
                    )]

            _total_rv = len(df)
            _shown_rv = len(df_rv)
            if search_rv:
                st.caption(f"🔍 {_shown_rv} نتيجة من أصل {_total_rv} منتج")
            else:
                st.caption(f"📋 {_total_rv} منتج للمراجعة")

            # ── عرض المقارنة جنباً إلى جنب ────────────────────────────────
            PAGE_SIZE = 15
            tp = max(1, (len(df_rv) + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="rv_pg") if tp > 1 else 1
            page_rv = df_rv.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_rv.iterrows():
                our_name   = str(row.get("المنتج",""))
                comp_name  = str(row.get("منتج_المنافس","—"))
                our_price  = safe_float(row.get("السعر",0))
                comp_price = safe_float(row.get("سعر_المنافس",0))
                score      = safe_float(row.get("نسبة_التطابق",0))
                brand      = str(row.get("الماركة",""))
                size       = str(row.get("الحجم",""))
                comp_name_s= str(row.get("المنافس",""))
                diff       = our_price - comp_price

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                _rv_our_img, _rv_comp_img = row_media_urls_from_analysis(row)
                _rv_our_thumb = lazy_img_tag(_rv_our_img, 56, 56, our_name[:40]) if _rv_our_img else ""
                _rv_comp_thumb = lazy_img_tag(_rv_comp_img, 56, 56, comp_name[:40]) if _rv_comp_img else ""
                _rv_our_url = our_product_url_from_row(row)
                _rv_comp_url = competitor_product_url_from_row(row)
                _rv_our_title = linked_product_title(
                    our_name[:60], _rv_our_url, color="#fff", font_size=".88rem",
                )
                _rv_comp_title = linked_product_title(
                    comp_name[:60], _rv_comp_url, color="#fff", font_size=".88rem",
                )

                # ── بطاقة المقارنة (مع صور عند التوفر) ─────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {brand} | 📏 {size}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:8px;align-items:stretch">
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733;
                                display:flex;align-items:flex-start;gap:10px;flex-direction:row-reverse">
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                        <div style="line-height:1.35">{_rv_our_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                      </div>
                      <div style="flex-shrink:0">{_rv_our_thumb}</div>
                    </div>
                    <div style="text-align:center;display:flex;flex-direction:column;justify-content:center">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233;
                                display:flex;align-items:flex-start;gap:10px">
                      <div style="flex-shrink:0">{_rv_comp_thumb}</div>
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {comp_name_s}</div>
                        <div style="line-height:1.35">{_rv_comp_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                      </div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba,bb,bc,bd,be = st.columns(5)

                with ba:
                    if st.button("🤖 تحقق AI", key=f"rv_verify_{idx}"):
                        with st.spinner("..."):
                            r_v = verify_match(our_name, comp_name, our_price, comp_price)
                            if r_v.get("success"):
                                conf = r_v.get("confidence",0)
                                match = r_v.get("match", False)
                                reason = str(r_v.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                reason = _re.sub(r'```.*?```','', reason, flags=_re.DOTALL)
                                reason = _re.sub(r'\{[^}]{0,200}\}','', reason).strip()
                                _lbl = "✅ نفس المنتج" if match else "❌ مختلف"
                                st.info(f"**{_lbl}** ({conf}%)\n{reason[:150]}")
                            else:
                                st.warning("فشل التحقق")

                with bb:
                    if st.button("✅ موافق", key=f"rv_approve_{idx}"):
                        log_decision(our_name,"review","approved","موافق",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "approved_from_review")
                        save_processed(_rv_key, our_name, comp_name_s, "approved",
                                       old_price=our_price, new_price=our_price,
                                       notes="موافق من تحت المراجعة")
                        st.rerun()

                with bc:
                    if st.button("🔴 سعر أعلى", key=f"rv_raise_{idx}"):
                        log_decision(our_name,"review","price_raise","سعر أعلى",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_price_raise")
                        save_processed(_rv_key, our_name, comp_name_s, "send_price",
                                       old_price=our_price, new_price=comp_price - 1 if comp_price > 0 else our_price,
                                       notes="نُقل من المراجعة → سعر أعلى")
                        st.rerun()

                with bd:
                    if st.button("🔵 مفقود", key=f"rv_missing_{idx}"):
                        log_decision(our_name,"review","missing","مفقود",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_missing")
                        save_processed(_rv_key, our_name, comp_name_s, "send_missing",
                                       new_price=comp_price,
                                       notes="نُقل من المراجعة → مفقود")
                        st.rerun()

                with be:
                    if st.button("🗑️ تجاهل", key=f"rv_ign_{idx}"):
                        log_decision(our_name,"review","ignored","تجاهل",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "ignored_review")
                        save_processed(_rv_key, our_name, comp_name_s, "ignored",
                                       old_price=our_price,
                                       notes="تجاهل من تحت المراجعة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")

# ════════════════════════════════════════════════
#  تمت المعالجة — v26
# ════════════════════════════════════════════════
elif page == "✔️ تمت المعالجة":
    st.header("✔️ المنتجات المعالجة")
    st.caption("جميع المنتجات التي تم ترحيلها أو تحديث سعرها أو إضافتها")
    db_log("processed", "view")

    processed = get_processed(limit=500)
    if not processed:
        st.info("📭 لا توجد منتجات معالجة بعد")
    else:
        df_proc = pd.DataFrame(processed)

        # إحصاء
        actions = df_proc["action"].value_counts()
        cols_p = st.columns(len(actions) + 1)
        for i, (act, cnt) in enumerate(actions.items()):
            icon = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}.get(act,"📌")
            cols_p[i].metric(f"{icon} {act}", cnt)
        cols_p[-1].metric("📦 الإجمالي", len(df_proc))

        # فلتر
        act_filter = st.selectbox("نوع الإجراء", ["الكل"] + list(actions.index))
        show_df = df_proc if act_filter == "الكل" else df_proc[df_proc["action"] == act_filter]

        st.markdown("---")

        for _, row in show_df.iterrows():
            p_key  = str(row.get("product_key",""))
            p_name = str(row.get("product_name",""))
            p_act  = str(row.get("action",""))
            p_ts   = str(row.get("timestamp",""))
            p_price_old = safe_float(row.get("old_price",0))
            p_price_new = safe_float(row.get("new_price",0))
            p_notes = str(row.get("notes",""))
            p_comp  = str(row.get("competitor",""))

            icon_map = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}
            icon = icon_map.get(p_act, "📌")

            col_a, col_b = st.columns([5, 1])
            with col_a:
                price_info = ""
                if p_price_old > 0 and p_price_new > 0:
                    price_info = f" | {p_price_old:.0f} → {p_price_new:.0f} ر.س"
                elif p_price_new > 0:
                    price_info = f" | {p_price_new:.0f} ر.س"
                _notes_html = ("<br><span style='color:#aaa;font-size:.73rem'>" + p_notes[:80] + "</span>") if p_notes else ""
                _arow = _find_analysis_row_for_processed(p_name)
                _p_our_u, _p_comp_u = _lookup_product_urls_from_analysis_session(p_name)
                _url_chips_html = _processed_row_url_chips_html(_p_our_u, _p_comp_u)
                _po, _pc = (
                    row_media_urls_from_analysis(_arow)
                    if _arow is not None
                    else ("", "")
                )
                # إن وُجد رابط صفحة بلا صورة في الجدول — جرّب og:image / أيقونة الموقع
                if (not _po) and (_p_our_u or "").strip().lower().startswith("http"):
                    _po = _cached_thumb_from_product_url(_p_our_u) or ""
                if (not _pc) and (_p_comp_u or "").strip().lower().startswith("http"):
                    _pc = _cached_thumb_from_product_url(_p_comp_u) or ""
                _comp_disp = (
                    str(_arow.get("منتج_المنافس", "") or "").strip()
                    if _arow is not None
                    else ""
                )
                if not _comp_disp:
                    _comp_disp = p_comp or "منافس"
                _thumb_cell = _processed_dual_image_html(_po, _pc, p_name[:100], _comp_disp[:100])
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:10px;padding:6px 10px;border-radius:6px;background:#0a1628;'
                    f'border:1px solid #1a2a44;font-size:.85rem">'
                    f'{_thumb_cell}'
                    f'<div style="flex:1;min-width:0">'
                    f'<span style="color:#888;font-size:.75rem">{p_ts[:16]}</span> &nbsp;'
                    f'{icon} <b style="color:#4fc3f7">{p_name[:60]}</b>'
                    f'<span style="color:#888"> — {p_act}{price_info}</span>'
                    f'{_notes_html}{_url_chips_html}</div></div>',
                    unsafe_allow_html=True
                )
            with col_b:
                if st.button("↩️ تراجع", key=f"undo_{p_key}"):
                    undo_processed(p_key)
                    # أعد للقائمة النشطة
                    if p_key in st.session_state.hidden_products:
                        st.session_state.hidden_products.discard(p_key)
                    st.success(f"✅ تم التراجع: {p_name[:40]}")
                    st.rerun()

        # تصدير
        st.markdown("---")
        csv_proc = df_proc.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📥 تصدير CSV", data=csv_proc,
                           file_name="processed_products.csv", mime="text/csv")



# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        st.markdown(f"{color} **{name}:** {r['message']}")
                if results.get("all_connected"):
                    st.success("✅ جميع الاتصالات تعمل")

    with tab2:
        if st.session_state.results:
            wh = st.selectbox("نوع الإرسال", ["سعر أعلى (تخفيض)","سعر أقل (رفع)","موافق عليها","مفقودة"])
            key_map = {
                "سعر أعلى (تخفيض)": "price_raise",
                "سعر أقل (رفع)":    "price_lower",
                "موافق عليها":      "approved",
                "مفقودة":           "missing",
            }
            section_type_map = {
                "price_raise": "raise",
                "price_lower": "lower",
                "approved":    "approved",
                "missing":     "missing",
            }
            sec_key  = key_map[wh]
            sec_type = section_type_map[sec_key]
            df_s     = st.session_state.results.get(sec_key, pd.DataFrame())

            if not df_s.empty:
                # معاينة ما سيُرسل
                _prev_cols = ["المنتج","السعر","سعر_المنافس","الماركة"]
                _prev_cols = [c for c in _prev_cols if c in df_s.columns]
                if _prev_cols:
                    st.dataframe(df_s[_prev_cols].head(10), use_container_width=True)

                products = export_to_make_format(df_s, sec_type)
                _sendable = [p for p in products if p.get("name") and p.get("price",0) > 0]
                st.info(f"سيتم إرسال {len(_sendable)} منتج → Make (Payload: product_id + name + price)")

                if st.button("📤 إرسال الآن", type="primary"):
                    if sec_type == "missing":
                        res = send_missing_products(_sendable)
                    else:
                        res = send_price_updates(_sendable)
                    st.success(res["message"]) if res["success"] else st.error(res["message"])
            else:
                st.info("لا توجد بيانات في هذا القسم")

    with tab3:
        pending = st.session_state.decisions_pending
        if pending:
            st.info(f"📦 {len(pending)} قرار معلق")
            df_p = pd.DataFrame([
                {"المنتج": k, "القرار": v["action"],
                 "وقت القرار": v.get("ts",""), "المنافس": v.get("competitor","")}
                for k, v in pending.items()
            ])
            st.dataframe(df_p.head(200), use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📤 إرسال كل القرارات لـ Make"):
                    to_send = [{"name": k, **v} for k, v in pending.items()]
                    res = send_price_updates(to_send)
                    st.success(res["message"])
                    st.session_state.decisions_pending = {}
                    st.rerun()
            with c2:
                if st.button("🗑️ مسح القرارات"):
                    st.session_state.decisions_pending = {}
                    st.rerun()
        else:
            st.info("لا توجد قرارات معلقة")


# ════════════════════════════════════════════════
#  11. كشط المنافسين (Async Scraper Dashboard)
# ════════════════════════════════════════════════
elif page == "🕷️ كشط المنافسين":
    import subprocess
    import sys as _sys_sc
    import os as _os_scraper
    import json as _json_sc
    from pathlib import Path as _Path

    st.header("🕷️ كشط بيانات المنافسين")
    db_log("scraper", "view")

    # ── مسارات ──────────────────────────────────────────────────────────
    _SC_ROOT      = _Path(__file__).resolve().parent
    _DATA_SC      = _os_scraper.environ.get("DATA_DIR", str(_SC_ROOT / "data"))
    _PROGRESS_FILE = _os_scraper.path.join(_DATA_SC, "scraper_progress.json")
    _OUTPUT_CSV    = _os_scraper.path.join(_DATA_SC, "competitors_latest.csv")
    _COMP_FILE     = _os_scraper.path.join(_DATA_SC, "competitors_list.json")
    _ERROR_LOG     = _os_scraper.path.join(_DATA_SC, "scraper_errors.log")
    _SCRAPER_SCRIPT = _os_scraper.path.join("scrapers", "async_scraper.py")

    # ── دوال مساعدة ─────────────────────────────────────────────────────
    def _load_stores() -> list:
        try:
            return _json_sc.loads(open(_COMP_FILE, encoding="utf-8").read())
        except Exception:
            return []

    def _save_stores(lst: list) -> None:
        _os_scraper.makedirs(_DATA_SC, exist_ok=True)
        open(_COMP_FILE, "w", encoding="utf-8").write(
            _json_sc.dumps(lst, ensure_ascii=False, indent=2)
        )

    def _load_progress() -> dict:
        try:
            return _json_sc.loads(open(_PROGRESS_FILE, encoding="utf-8").read())
        except Exception:
            return {"running": False}

    def _read_error_log(max_lines: int = 60) -> str:
        """يقرأ آخر سطور من ملف السجل."""
        try:
            with open(_ERROR_LOG, encoding="utf-8", errors="replace") as _f:
                lines = _f.readlines()
            return "".join(lines[-max_lines:]) if lines else ""
        except FileNotFoundError:
            return ""
        except Exception as _ex:
            return f"(تعذّر قراءة السجل: {_ex})"

    def _count_csv_rows() -> int:
        try:
            with open(_OUTPUT_CSV, encoding="utf-8-sig") as _f:
                return max(0, sum(1 for _ in _f) - 1)
        except Exception:
            return 0

    # ══ Callbacks ═══════════════════════════════════════════════════════
    def _cb_add_store():
        url = (st.session_state.get("sc_new_url") or "").strip()
        if not url:
            return
        if not url.startswith("http"):
            url = "https://" + url
        lst = _load_stores()
        if url not in lst:
            lst.append(url)
            _save_stores(lst)
            st.session_state["_sc_msg"] = ("success", f"تمت إضافة: {url}")
        else:
            st.session_state["_sc_msg"] = ("warning", "الرابط موجود مسبقاً")
        st.session_state["sc_new_url"] = ""

    def _cb_remove_store(idx_to_remove: int):
        lst = _load_stores()
        if 0 <= idx_to_remove < len(lst):
            removed = lst.pop(idx_to_remove)
            _save_stores(lst)
            st.session_state["_sc_msg"] = ("success", f"تم حذف: {removed}")

    def _start_scraper_bg(full_mode: bool = False):
        _os_scraper.makedirs(_DATA_SC, exist_ok=True)
        try:
            # فتح ملف السجل بوضع الإضافة (append) — لا يُمحى
            _log_fh = open(_ERROR_LOG, "a", encoding="utf-8")
            _cmd = [
                _sys_sc.executable, "-m", "scrapers.async_scraper",
                "--max-products", str(
                    0 if st.session_state.get("sc_all_products", True)
                    else int(st.session_state.get("sc_max_prod", 0) or 0)
                ),
                "--concurrency", str(int(st.session_state.get("sc_concurrency", 8))),
            ]
            if full_mode:
                _cmd.append("--full")
            subprocess.Popen(
                _cmd,
                stdout=_log_fh,
                stderr=_log_fh,
                start_new_session=True,
                cwd=str(_SC_ROOT),        # ← حاسم: يضمن import scrapers.* يعمل
            )
            st.session_state["_sc_started"] = True
        except Exception as _exc:
            st.session_state["_sc_err"] = str(_exc)

    def _cb_start():      _start_scraper_bg(full_mode=False)
    def _cb_start_full(): _start_scraper_bg(full_mode=True)

    # ══ رسائل الـ Callbacks ══════════════════════════════════════════════
    if _sc_msg := st.session_state.pop("_sc_msg", None):
        getattr(st, _sc_msg[0])(_sc_msg[1])
    if st.session_state.pop("_sc_started", False):
        st.success("بدأ الكشط في الخلفية — راقب التقدم أدناه وشاهد سجل الأخطاء")
    if _sc_err := st.session_state.pop("_sc_err", None):
        st.error(f"فشل تشغيل الكاشط: {_sc_err}")

    _prog_now  = _load_progress()
    _is_running = bool(_prog_now.get("running", False))

    # ══════════════════════════════════════════════════════════════════════
    # ملخص سريع: قائمة المنافسين المُعرَّفة (تظهر دائماً فوق التبويبات)
    # ══════════════════════════════════════════════════════════════════════
    _quick_stores = _load_stores()
    if _quick_stores:
        _qs_html_items = "".join(
            f'<span style="display:inline-flex;align-items:center;gap:4px;'
            f'background:#1a1a3a;border:1px solid #2d2d5a;border-radius:20px;'
            f'padding:3px 10px;font-size:.78rem;color:#aaa;white-space:nowrap">'
            f'<span style="color:#6C63FF;font-weight:700">{_qi+1}</span> '
            f'{_qu.replace("https://","").replace("http://","").rstrip("/").split("/")[0]}'
            f'</span>'
            for _qi, _qu in enumerate(_quick_stores)
        )
        _running_badge = (
            '<span style="background:#0a3a1a;border:1px solid #00C853;border-radius:12px;'
            'padding:2px 10px;font-size:.75rem;color:#00C853">⚡ يعمل الآن</span>'
            if _is_running else
            '<span style="background:#1a1a2a;border:1px solid #444;border-radius:12px;'
            'padding:2px 10px;font-size:.75rem;color:#888">● متوقف</span>'
        )
        st.markdown(
            f'<div style="background:#0d0d1f;border:1px solid #2a2a4a;border-radius:10px;'
            f'padding:10px 14px;margin-bottom:10px">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">'
            f'<b style="color:#6C63FF">🏪 المنافسون ({len(_quick_stores)} متجر)</b>'
            f'{_running_badge}</div>'
            f'<div style="display:flex;flex-wrap:wrap;gap:5px">{_qs_html_items}</div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.info("لا توجد متاجر منافسين بعد — أضفها من تبويب «🌐 المتاجر» أدناه")

    # ══════════════════════════════════════════════════════════════════════
    # تقسيم الصفحة إلى تبويبات منظمة
    # ══════════════════════════════════════════════════════════════════════
    _tab_stores, _tab_run, _tab_monitor, _tab_log, _tab_sched = st.tabs([
        "🌐 المتاجر",
        "▶️ تشغيل",
        "📊 المراقبة",
        "🪲 سجل الأخطاء",
        "⏰ الجدولة",
    ])

    # ══════════════════════════════════
    # تبويب 1: إدارة المتاجر
    # ══════════════════════════════════
    with _tab_stores:
        st.subheader("إدارة متاجر المنافسين")
        _col_url, _col_add = st.columns([4, 1])
        with _col_url:
            st.text_input(
                "رابط متجر جديد",
                placeholder="https://example.com  أو  example.com",
                key="sc_new_url",
                label_visibility="collapsed",
            )
        with _col_add:
            st.button("➕ إضافة", on_click=_cb_add_store, key="btn_add_store",
                      width='stretch')

        _stores_list = _load_stores()
        if _stores_list:
            st.caption(f"{len(_stores_list)} متجر مستهدف")
            for _si, _surl in enumerate(_stores_list):
                _r1, _r2 = st.columns([7, 1])
                with _r1:
                    st.markdown(
                        f'<div style="padding:6px 10px;background:#12122a;'
                        f'border:1px solid #2a2a4a;border-radius:6px;font-size:.84rem">'
                        f'<b style="color:#6C63FF">{_si+1}.</b> {_surl}</div>',
                        unsafe_allow_html=True,
                    )
                with _r2:
                    st.button("🗑️", key=f"del_{_si}", on_click=_cb_remove_store,
                              args=(_si,), width='stretch', help=f"حذف {_surl}")
        else:
            st.warning("لا توجد متاجر — أضف رابطاً للبدء")
            st.info(
                "أمثلة:\n"
                "- `https://worldgivenchy.com`\n"
                "- `https://saeedsalah.com`\n"
                "- `https://example.salla.sa`"
            )

    # ══════════════════════════════════
    # تبويب 2: تشغيل الكاشط
    # ══════════════════════════════════
    with _tab_run:
        st.subheader("إعدادات وتشغيل الكاشط")

        _stores_count = len(_load_stores())
        if not _stores_count:
            st.warning("لا توجد متاجر — أضف من تبويب «المتاجر» أولاً")
        else:
            _sc_col1, _sc_col2, _sc_col3 = st.columns(3)
            with _sc_col1:
                st.checkbox(
                    "جميع المنتجات (بلا سقف)",
                    value=True, key="sc_all_products",
                    help="يكشط كل منتج في Sitemap بدون حد",
                )
            with _sc_col2:
                st.number_input(
                    "أقصى منتجات / متجر",
                    0, 50000,
                    0 if st.session_state.get("sc_all_products", True) else 1000,
                    step=500, key="sc_max_prod",
                    disabled=bool(st.session_state.get("sc_all_products", True)),
                )
            with _sc_col3:
                st.number_input("طلبات متزامنة", 1, 30, 3, step=1, key="sc_concurrency",
                                help="3 هو الأنسب لمعظم المتاجر المحمية")

            _limit    = int(st.session_state.get("sc_max_prod", 0) or 0)
            _all_flag = bool(st.session_state.get("sc_all_products", True))
            if _all_flag or _limit == 0:
                st.info(f"سيتم كشط **جميع المنتجات** من {_stores_count} متجر")
            else:
                st.info(f"تقدير: {_stores_count * _limit:,} منتج")

            st.markdown("---")
            _btn_a, _btn_b = st.columns(2)
            with _btn_a:
                st.button(
                    "⏳ الكشط يعمل بالفعل…" if _is_running else "🚀 بدء الكشط (تزايدي)",
                    type="primary", on_click=_cb_start,
                    key="btn_start_incr", width='stretch',
                    disabled=_is_running,
                    help="يكشط فقط المنتجات الجديدة أو المحدَّثة — البيانات القديمة تُحفظ",
                )
            with _btn_b:
                st.button(
                    "🔄 كشط كامل (يُعيد الكل)",
                    type="secondary", on_click=_cb_start_full,
                    key="btn_start_full", width='stretch',
                    disabled=_is_running,
                    help="يكشط جميع المنتجات من جديد بغض النظر عن التغييرات",
                )

            st.caption(
                "**البيانات القديمة لا تُحذف:** كل جلسة كشط تُضيف وتُحدِّث فقط، "
                "ولا تمحو ما سبق جمعه من متاجر أخرى."
            )

    # ══════════════════════════════════
    # تبويب 3: المراقبة الحية
    # ══════════════════════════════════
    with _tab_monitor:
        if _is_running:
            try:
                from streamlit_autorefresh import st_autorefresh
                st_autorefresh(interval=3000, key="sc_autorefresh")
            except ImportError:
                st.caption("نصيحة: ثبّت `streamlit-autorefresh` للتحديث التلقائي")

        _sl_mon = _load_stores()
        if not _sl_mon:
            st.warning("لا توجد متاجر — أضف متاجر من تبويب «🌐 المتاجر» أولاً")
        elif not _os_scraper.path.exists(_PROGRESS_FILE):
            st.info(f"لم تبدأ أي عملية كشط بعد. ({len(_sl_mon)} متجر جاهز في القائمة)")
        else:
            _prog      = _load_progress()
            _running   = bool(_prog.get("running", False))
            _rows      = int(_prog.get("rows_in_csv", 0))
            _errors    = int(_prog.get("fetch_exceptions", 0))
            _success   = float(_prog.get("success_rate_pct", 0))
            _current   = str(_prog.get("current_store", ""))
            _last_err  = str(_prog.get("last_error", ""))
            _s_done    = int(_prog.get("stores_done", 0))
            # استخدم عدد المتاجر الفعلي من الملف (لا من progress القديم)
            _s_tot     = max(int(_prog.get("stores_total", 0)), len(_load_stores()), 1)
            _u_done    = int(_prog.get("store_urls_done", 0))
            _u_tot     = max(int(_prog.get("store_urls_total", 1)), 1)
            _s_res     = dict(_prog.get("stores_results") or {})

            # حالة الجلسة
            if _running:
                st.markdown(
                    f'<div style="background:#0a1a2a;border:1px solid #4fc3f7;'
                    f'border-radius:8px;padding:10px 14px;margin-bottom:10px">'
                    f'🔄 <b>يعمل الآن</b> — '
                    f'<b style="color:#4fc3f7">{_current or "…"}</b>'
                    f'<span style="color:#9e9e9e"> ({_s_done + 1}/{_s_tot})</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                _fin = _prog.get("finished_at", "")
                _csv_total = _count_csv_rows()
                st.success(
                    f"اكتملت الجلسة الأخيرة — "
                    f"{_rows:,} منتج جديد/محدَّث | إجمالي CSV: {_csv_total:,}"
                    + (f" | {_fin[:16]}" if _fin else "")
                )

            # شريط المتاجر
            _sp = min(_s_done / _s_tot, 1.0)
            st.progress(_sp, text=f"المتاجر: {_s_done}/{_s_tot} ({_sp*100:.0f}%)")

            # شريط المتجر الحالي
            if _running and _u_tot > 1:
                _up = min(_u_done / _u_tot, 1.0)
                st.progress(_up, text=f"{_current}: {_u_done:,}/{_u_tot:,} رابط ({_up*100:.0f}%)")

            # بطاقات
            _c1, _c2, _c3, _c4 = st.columns(4)
            _c1.metric("إجمالي CSV", f"{_count_csv_rows():,}")
            _c2.metric("جلسة هذه", f"{_rows:,}")
            _c3.metric("نسبة النجاح", f"{_success:.1f}%")
            _c4.metric("أخطاء", str(_errors))

            # تفاصيل المتاجر
            _sl = _load_stores()
            _sitemap_failed = list(_prog.get("stores_sitemap_failed") or [])
            if _sl:
                st.markdown("**تفاصيل المتاجر:**")
                _items = []
                for _i, _u in enumerate(_sl):
                    _d = _u.replace("https://", "").replace("http://", "").rstrip("/").split("/")[0]
                    _cnt = _s_res.get(_d)
                    if _d == _current and _running:
                        # 🔄 يعمل الآن
                        _bw = int(min(_u_done / _u_tot, 1.0) * 100) if _u_tot > 1 else 0
                        _items.append(
                            f'<div style="background:#0a1a2a;border:1px solid #4fc3f7;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'🔄 <b style="color:#4fc3f7">{_i+1}. {_d}</b>'
                            f'<span style="color:#9e9e9e"> — {_u_done:,}/{_u_tot:,}</span>'
                            f'<div style="margin-top:4px;height:4px;background:#1a2a3a;border-radius:2px">'
                            f'<div style="width:{_bw}%;height:100%;background:#4fc3f7;border-radius:2px"></div>'
                            f'</div></div>'
                        )
                    elif _cnt is not None and _cnt > 0:
                        # ✅ كُشط ومنتجات جديدة
                        _items.append(
                            f'<div style="background:#0a1a0a;border:1px solid #1e3a1e;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'✅ <span style="color:#aaa">{_i+1}. {_d}</span>'
                            f'<span style="color:#00C853"> — {_cnt:,} منتج جديد/محدَّث</span></div>'
                        )
                    elif _cnt is not None and _cnt < 0:
                        # 💾 محفوظ مسبقاً — بلا تحديث (lastmod لم يتغير)
                        _saved = abs(_cnt)
                        _items.append(
                            f'<div style="background:#0d1a0d;border:1px solid #1a3a1a;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'💾 <span style="color:#81c784">{_i+1}. {_d}</span>'
                            f'<span style="color:#66bb6a"> — {_saved:,} منتج محفوظ (بلا تحديث)</span></div>'
                        )
                    elif _cnt == 0 and _d in _sitemap_failed:
                        # ⚠️ فشل Sitemap — لا روابط منتجات
                        _items.append(
                            f'<div style="background:#1a0a0a;border:1px solid #3a1a1a;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'⚠️ <span style="color:#ef9a9a">{_i+1}. {_d}</span>'
                            f'<span style="color:#e57373"> — فشل استخراج Sitemap</span></div>'
                        )
                    elif _cnt == 0:
                        # ✅ انتهى — صفر منتج جديد (قد يكون محفوظاً من قبل)
                        _items.append(
                            f'<div style="background:#0a1a0a;border:1px dashed #2a3a2a;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'✅ <span style="color:#666">{_i+1}. {_d} — 0 منتج جديد</span></div>'
                        )
                    elif _i < _s_done:
                        _items.append(
                            f'<div style="background:#0a1a0a;border:1px dashed #2a3a2a;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'✅ <span style="color:#555">{_i+1}. {_d}</span></div>'
                        )
                    elif _running:
                        _items.append(
                            f'<div style="background:#111;border:1px dashed #333;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'⏳ <span style="color:#555">{_i+1}. {_d}</span></div>'
                        )
                    else:
                        _items.append(
                            f'<div style="background:#0d0d0d;border:1px solid #222;'
                            f'border-radius:6px;padding:7px 12px;font-size:.82rem">'
                            f'⬜ <span style="color:#666">{_i+1}. {_d}</span></div>'
                        )
                st.markdown(
                    '<div style="display:flex;flex-direction:column;gap:4px">'
                    + "".join(_items) + "</div>",
                    unsafe_allow_html=True,
                )

            if _last_err:
                st.error(f"آخر خطأ مسجَّل: {_last_err}")

            # تحميل الناتج
            st.markdown("---")
            if _os_scraper.path.exists(_OUTPUT_CSV):
                _csv_sz  = round(_os_scraper.path.getsize(_OUTPUT_CSV) / 1024, 1)
                _csv_cnt = _count_csv_rows()
                _dl2, _go2 = st.columns(2)
                with _dl2:
                    with open(_OUTPUT_CSV, "rb") as _fout:
                        st.download_button(
                            f"📥 تحميل CSV ({_csv_sz} KB · {_csv_cnt:,} منتج)",
                            data=_fout.read(),
                            file_name="competitors_latest.csv",
                            mime="text/csv",
                            key="sc_dl_mon",
                            width='stretch',
                        )
                with _go2:
                    if st.button("🚀 انتقل للمطابقة", key="sc_go_mon",
                                 type="primary", width='stretch'):
                        st.session_state._nav_pending = "📊 لوحة التحكم"
                        st.session_state["_use_auto_scraper"] = True
                        st.rerun()
            else:
                st.info("لم يُنتج ملف بعد.")

            st.button("🔄 تحديث يدوي", key="sc_refresh_mon")

    # ══════════════════════════════════
    # تبويب 4: سجل الأخطاء
    # ══════════════════════════════════
    with _tab_log:
        st.subheader("سجل العمليات والأخطاء")
        st.caption(
            f"الملف: `{_ERROR_LOG}` — يُضاف إليه ولا يُمحى بين الجلسات"
        )

        _log_text = _read_error_log(max_lines=100)
        if _log_text:
            # تمييز الأخطاء بلون مختلف
            _log_lines = _log_text.splitlines()
            _err_lines = [l for l in _log_lines if "[ERROR]" in l or "[WARNING]" in l or "Traceback" in l or "Error" in l]
            _info_cnt  = len(_log_lines) - len(_err_lines)

            _lc1, _lc2, _lc3 = st.columns(3)
            _lc1.metric("أسطر في السجل", len(_log_lines))
            _lc2.metric("أخطاء/تحذيرات", len(_err_lines))
            _lc3.metric("معلومات", _info_cnt)

            if _err_lines:
                with st.expander(f"عرض الأخطاء فقط ({len(_err_lines)} سطر)", expanded=True):
                    st.code("\n".join(_err_lines[-40:]), language=None)

            with st.expander("السجل الكامل (آخر 100 سطر)", expanded=not _err_lines):
                st.code(_log_text, language=None)

            # زر تحميل السجل الكامل
            with open(_ERROR_LOG, "rb") as _lf:
                st.download_button(
                    "📥 تحميل السجل الكامل",
                    data=_lf.read(),
                    file_name="scraper_errors.log",
                    mime="text/plain",
                    key="sc_dl_log",
                )
        else:
            st.info("السجل فارغ — لم تبدأ أي عملية كشط بعد، أو السجل جديد.")

        if st.button("🗑️ مسح السجل", key="sc_clear_log"):
            try:
                open(_ERROR_LOG, "w", encoding="utf-8").close()
                st.success("تم مسح السجل")
                st.rerun()
            except Exception as _ex:
                st.error(f"فشل المسح: {_ex}")

    # ══════════════════════════════════
    # تبويب 5: الجدولة التلقائية
    # ══════════════════════════════════
    with _tab_sched:
        st.subheader("الجدولة التلقائية")
        try:
            from scrapers.scheduler import (
                get_scheduler_status, enable_scheduler, disable_scheduler,
                trigger_now as _trigger_now,
            )
            _sch        = get_scheduler_status()
            _sch_on     = bool(_sch.get("enabled", False))
            _sch_h      = int(_sch.get("interval_hours", 12))
            _sch_runs   = int(_sch.get("runs_count", 0))
            _sch_last   = str(_sch.get("last_run", "") or "لم يعمل بعد")[:19]
            _sch_next   = _sch.get("next_run_label", "—")

            if _sch_on:
                st.markdown(
                    f'<div style="background:#0a2a0a;border:1px solid #00C853;'
                    f'border-radius:8px;padding:12px 16px">'
                    f'🤖 <b>الكشط التلقائي مُفعَّل</b><br>'
                    f'<span style="color:#9e9e9e;font-size:.83rem">'
                    f'كل {_sch_h} ساعة | القادم: <b style="color:#4fc3f7">{_sch_next}</b> | '
                    f'عدد التشغيلات: {_sch_runs}</span></div>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<div style="background:#1a1a1a;border:1px dashed #555;'
                    'border-radius:8px;padding:12px 16px">'
                    '⏸️ <b>الكشط التلقائي معطَّل</b><br>'
                    '<span style="color:#9e9e9e;font-size:.83rem">'
                    'فعّله لكشط المنافسين آلياً</span></div>',
                    unsafe_allow_html=True,
                )

            _sh_c1, _sh_c2 = st.columns([3, 2])
            with _sh_c1:
                st.number_input("تكرار (ساعات)", 1, 168, _sch_h, step=1, key="sc_interval_h")
            with _sh_c2:
                if _sch_last != "لم يعمل بعد":
                    st.caption(f"آخر تشغيل: {_sch_last[:10]}")

            def _cb_toggle_sch():
                _h = int(st.session_state.get("sc_interval_h", 12))
                if not _sch_on:
                    enable_scheduler(interval_hours=_h)
                    st.session_state["_sc_msg"] = ("success", f"الجدولة مُفعَّلة — كل {_h} ساعة")
                else:
                    disable_scheduler()
                    st.session_state["_sc_msg"] = ("warning", "الجدولة مُعطَّلة")

            def _cb_run_sched_now():
                _mp = 0 if st.session_state.get("sc_all_products", True) else int(
                    st.session_state.get("sc_max_prod", 0) or 0)
                _cc = int(st.session_state.get("sc_concurrency", 8))
                ok = _trigger_now(max_products=_mp, concurrency=_cc)
                st.session_state["_sc_msg"] = (
                    "success" if ok else "error",
                    "تم إطلاق الكشط الآن!" if ok else "فشل تشغيل الكاشط"
                )

            _sb1, _sb2 = st.columns(2)
            with _sb1:
                st.button(
                    "⏸️ تعطيل" if _sch_on else "▶️ تفعيل الجدولة",
                    on_click=_cb_toggle_sch, key="btn_tog_sch",
                    type="primary" if not _sch_on else "secondary",
                    width='stretch',
                )
            with _sb2:
                st.button(
                    "🚀 تشغيل الآن",
                    on_click=_cb_run_sched_now, key="btn_run_sch_now",
                    width='stretch', disabled=_is_running,
                )

        except Exception as _sch_ex:
            st.error(f"تعذّر تحميل وحدة الجدولة: {_sch_ex}")


# ════════════════════════════════════════════════
#  10. الإعدادات
# ════════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["🔑 المفاتيح", "⚙️ المطابقة", "📋 قرارات المنتجات", "📜 السجل الكامل"]
    )

    with tab1:
        # ── الحالة الحالية ────────────────────────────────────────────────
        st.success(
            "✅ **مسار AI جاهز** (Gemini و/أو OpenRouter و/أو Cohere)"
            if ANY_AI_PROVIDER_CONFIGURED
            else "❌ **لا يوجد أي مزود** — أضف مفتاحاً على الأقل"
        )
        gemini_s = f"✅ {len(GEMINI_API_KEYS)} مفتاح" if GEMINI_API_KEYS else "❌ لا توجد مفاتيح"
        or_s     = "✅ مفعل" if OPENROUTER_API_KEY else "❌ غير موجود"
        co_s     = "✅ مفعل" if COHERE_API_KEY else "❌ غير موجود"
        st.info(f"Gemini API: {gemini_s}")
        st.info(f"OpenRouter: {or_s}")
        st.info(f"Cohere:     {co_s}")
        st.info(f"Webhook أسعار:   {'✅' if WEBHOOK_UPDATE_PRICES else '❌'}")
        st.info(f"Webhook منتجات:  {'✅' if WEBHOOK_NEW_PRODUCTS else '❌'}")

        st.markdown("---")

        # ── تشخيص شامل ───────────────────────────────────────────────────
        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers
                diag = diagnose_ai_providers()

            # ── نتائج Gemini ──────────────────────────────────────────────
            st.markdown("**Gemini API:**")
            any_gemini_ok = False
            for g in diag.get("gemini", []):
                status = g["status"]
                if "✅" in status:
                    st.success(f"مفتاح {g['key']}: {status}")
                    any_gemini_ok = True
                elif "⚠️" in status:
                    st.warning(f"مفتاح {g['key']}: {status}")
                else:
                    st.error(f"مفتاح {g['key']}: {status}")
                _gd = (g.get("detail") or "").strip()
                if _gd and ("❌" in status or "⚠️" in status):
                    st.caption(f"تفاصيل API: {_gd[:500]}")

            # ── نتائج OpenRouter ──────────────────────────────────────────
            or_res = diag.get("openrouter","")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res: st.success(or_res)
            elif "⚠️" in or_res: st.warning(or_res)
            else: st.error(or_res)

            # ── نتائج Cohere ──────────────────────────────────────────────
            co_res = diag.get("cohere","")
            st.markdown("**Cohere:**")
            if "✅" in co_res: st.success(co_res)
            elif "⚠️" in co_res: st.warning(co_res)
            else: st.error(co_res)

            # ── تحليل وتوصية ─────────────────────────────────────────────
            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

            _recs = diag.get("recommendations") or []
            if _recs:
                st.markdown("**💡 توصيات تلقائية (حسب نتيجة التشخيص)**")
                for _r in _recs:
                    st.info(_r)

            st.markdown("---")
            if any_gemini_ok or or_ok or co_ok:
                working = []
                if any_gemini_ok: working.append("Gemini")
                if or_ok: working.append("OpenRouter")
                if co_ok: working.append("Cohere")
                st.success(f"✅ AI يعمل عبر: {' + '.join(working)}")
            else:
                st.error("❌ جميع المزودين فاشلون")
                # تحليل السبب
                _all_errs = [g["status"] for g in diag.get("gemini",[]) if "❌" in g.get("status","")]
                if any("اتصال" in e or "ConnectionError" in e or "Pool" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("""
**🔴 السبب المحتمل: Streamlit Cloud يحجب الطلبات الخارجية**

الحل: في صفحة تطبيقك على Streamlit Cloud:
1. اذهب إلى ⚙️ Settings → General
2. ابحث عن **"Network"** أو **"Egress"**
3. تأكد أن Outbound connections مسموح بها

أو جرب نشر التطبيق على **Railway** بدلاً من Streamlit Cloud.
                    """)
                elif any("403" in e or "IP" in e for e in _all_errs):
                    st.warning("🔴 مفاتيح Gemini محظورة من IP هذا الخادم — جرب OpenRouter")
                elif any("401" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("🔴 مفتاح غير صحيح — تحقق من المفاتيح في Secrets")

        st.markdown("---")

        # ── سجل الأخطاء الأخيرة ──────────────────────────────────────────
        st.subheader("📋 آخر أخطاء AI")
        from engines.ai_engine import get_last_errors
        errs = get_last_errors()
        if errs:
            for e in errs:
                st.code(e, language=None)
        else:
            st.caption("لا أخطاء مسجلة بعد — جرب أي زر AI ثم ارجع هنا")

        st.markdown("---")

        # ── اختبار سريع ──────────────────────────────────────────────────
        if st.button("🧪 اختبار سريع"):
            with st.spinner("يتصل بـ AI..."):
                r = call_ai("أجب بكلمة واحدة فقط: يعمل", "general")
            if r["success"]:
                st.success(f"✅ AI يعمل عبر {r['source']}: {r['response'][:80]}")
            else:
                st.error("❌ فشل — اضغط 'تشخيص شامل' لمعرفة السبب الدقيق")
                from engines.ai_engine import get_last_errors
                for e in get_last_errors()[:5]:
                    st.code(e, language=None)

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(df_dec[["timestamp","product_name","old_status",
                                  "new_status","reason","competitor"]].rename(columns={
                "timestamp":"التاريخ","product_name":"المنتج",
                "old_status":"من","new_status":"إلى",
                "reason":"السبب","competitor":"المنافس"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة")

    with tab4:
        db_log("settings", "full_log")
        st.caption("سجل التحليلات، تتبع الأسعار، وأحداث التنقل — مدمج مع الإعدادات")
        log_t1, log_t2, log_t3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

        with log_t1:
            history = get_analysis_history(20)
            if history:
                df_h = pd.DataFrame(history)
                st.dataframe(df_h[["timestamp","our_file","comp_file",
                                    "total_products","matched","missing"]].rename(columns={
                    "timestamp":"التاريخ","our_file":"ملف منتجاتنا",
                    "comp_file":"ملف المنافس","total_products":"الإجمالي",
                    "matched":"متطابق","missing":"مفقود"
                }).head(200), use_container_width=True)
            else:
                st.info("لا يوجد تاريخ")

        with log_t2:
            days = st.slider("آخر X يوم", 1, 30, 7, key="settings_price_changes_days")
            changes = get_price_changes(days)
            if changes:
                df_c = pd.DataFrame(changes)
                st.dataframe(df_c.rename(columns={
                    "product_name":"المنتج","competitor":"المنافس",
                    "old_price":"السعر السابق","new_price":"السعر الجديد",
                    "price_diff":"التغيير","new_date":"تاريخ التغيير"
                }).head(200), use_container_width=True)
            else:
                st.info(f"لا توجد تغييرات في آخر {days} يوم")

        with log_t3:
            events = get_events(limit=50)
            if events:
                df_e = pd.DataFrame(events)
                st.dataframe(df_e[["timestamp","page","event_type","details"]].rename(columns={
                    "timestamp":"التاريخ","page":"الصفحة",
                    "event_type":"الحدث","details":"التفاصيل"
                }).head(200), use_container_width=True)
            else:
                st.info("لا توجد أحداث")


# ════════════════════════════════════════════════
#  12. الأتمتة الذكية (v26.0 — متصل بالتنقل)
# ════════════════════════════════════════════════
elif page == "🔄 الأتمتة الذكية":
    st.header("🔄 الأتمتة الذكية — محرك القرارات التلقائية")
    db_log("automation", "view")

    # ── إنشاء محرك الأتمتة ──
    if "auto_engine" not in st.session_state:
        st.session_state.auto_engine = AutomationEngine()
    if "search_manager" not in st.session_state:
        st.session_state.search_manager = ScheduledSearchManager()

    engine = st.session_state.auto_engine
    search_mgr = st.session_state.search_manager

    tab_a1, tab_a2, tab_a3, tab_a4 = st.tabs([
        "🤖 تشغيل الأتمتة", "⚙️ قواعد التسعير", "🔍 البحث الدوري", "📊 سجل القرارات"
    ])

    # ── تاب 1: تشغيل الأتمتة ──
    with tab_a1:
        st.subheader("تطبيق القواعد التلقائية على نتائج التحليل")

        if st.session_state.results and st.session_state.analysis_df is not None:
            adf = st.session_state.analysis_df
            matched_df = adf[adf["نسبة_التطابق"].apply(lambda x: safe_float(x)) >= 85].copy()
            st.info(f"📦 {len(matched_df)} منتج مؤكد المطابقة جاهز للتقييم التلقائي")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🚀 تشغيل الأتمتة الآن", type="primary", key="run_auto"):
                    with st.spinner("⚙️ محرك الأتمتة يقيّم المنتجات..."):
                        engine.clear_log()
                        decisions = engine.evaluate_batch(matched_df)
                        st.session_state._auto_decisions = decisions

                        # تسجيل كل قرار في قاعدة البيانات
                        for d in decisions:
                            log_automation_decision(d)

                    if decisions:
                        summary = engine.get_summary()
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("إجمالي القرارات", summary["total"])
                        c2.metric("⬇️ خفض سعر", summary["lower"])
                        c3.metric("⬆️ رفع سعر", summary["raise"])
                        c4.metric("✅ إبقاء", summary["keep"])

                        if summary["net_impact"] > 0:
                            st.success(f"💰 الأثر المالي المتوقع: +{summary['net_impact']:.0f} ر.س (صافي ربح إضافي)")
                        elif summary["net_impact"] < 0:
                            st.warning(f"📉 الأثر المالي: {summary['net_impact']:.0f} ر.س (خفض لتحقيق التنافسية)")

                        # عرض القرارات في جدول
                        dec_df = pd.DataFrame(decisions)
                        display_cols = ["product_name", "action", "old_price", "new_price",
                                        "comp_price", "competitor", "match_score", "reason"]
                        available = [c for c in display_cols if c in dec_df.columns]
                        st.dataframe(dec_df[available].rename(columns={
                            "product_name": "المنتج", "action": "الإجراء",
                            "old_price": "السعر الحالي", "new_price": "السعر الجديد",
                            "comp_price": "سعر المنافس", "competitor": "المنافس",
                            "match_score": "نسبة التطابق", "reason": "السبب"
                        }), use_container_width=True)
                    else:
                        st.info("لم يتم اتخاذ أي قرارات — جميع الأسعار ضمن الهامش المقبول")

            with col_b:
                auto_decisions = st.session_state.get("_auto_decisions", [])
                push_eligible = [d for d in auto_decisions
                                 if d.get("action") in ("lower_price", "raise_price")
                                 and d.get("product_id")]
                if push_eligible:
                    st.warning(f"📤 {len(push_eligible)} قرار جاهز للإرسال إلى Make.com/سلة")
                    if st.button("📤 إرسال القرارات إلى Make.com", key="push_auto"):
                        with st.spinner("يُرسل إلى Make.com..."):
                            result = auto_push_decisions(auto_decisions)
                        if result.get("success"):
                            st.success(result["message"])
                        else:
                            st.error(result["message"])
                else:
                    st.caption("لا توجد قرارات جاهزة للإرسال — شغّل الأتمتة أولاً")

        else:
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات من أسفل «لوحة التحكم» ثم ابدأ التحليل")

        # ── معالجة قسم المراجعة تلقائياً ──
        st.divider()
        st.subheader("🔄 معالجة قسم المراجعة تلقائياً")
        st.caption("يستخدم AI للتحقق المزدوج من المطابقات غير المؤكدة")

        if st.session_state.results and "review" in st.session_state.results:
            rev_df = st.session_state.results.get("review", pd.DataFrame())
            if not rev_df.empty:
                st.info(f"📋 {len(rev_df)} منتج تحت المراجعة")
                if st.button("🤖 تحقق AI تلقائي لقسم المراجعة", key="auto_review"):
                    with st.spinner("🤖 AI يتحقق من المطابقات..."):
                        confirmed = auto_process_review_items(rev_df.head(15))
                    if not confirmed.empty:
                        st.success(f"✅ تم تأكيد {len(confirmed)} منتج من أصل {min(15, len(rev_df))}")
                        st.dataframe(confirmed[["المنتج", "منتج_المنافس", "القرار"]].head(20),
                                     use_container_width=True)
                    else:
                        st.info("لم يتم تأكيد أي مطابقة — المنتجات تحتاج مراجعة يدوية")
            else:
                st.success("لا توجد منتجات تحت المراجعة")

    # ── تاب 2: قواعد التسعير ──
    with tab_a2:
        st.subheader("⚙️ قواعد التسعير النشطة")
        st.caption("القواعد تُطبّق بالترتيب — أول قاعدة تنطبق تُنفَّذ")

        for i, rule in enumerate(engine.rules):
            with st.expander(f"{'✅' if rule.enabled else '⬜'} {rule.name}", expanded=False):
                st.write(f"**الإجراء:** {rule.action}")
                st.write(f"**حد التطابق الأدنى:** {rule.min_match_score}%")
                for k, v in rule.params.items():
                    if k not in ("name", "enabled", "action", "min_match_score", "condition"):
                        st.write(f"**{k}:** {v}")

        st.divider()
        st.subheader("📝 تخصيص القواعد")
        st.caption("يمكنك تعديل القواعد من ملف config.py → AUTOMATION_RULES_DEFAULT")
        st.code("""
# مثال: إضافة قاعدة جديدة في config.py
AUTOMATION_RULES_DEFAULT.append({
    "name": "خفض عدواني",
    "enabled": True,
    "action": "undercut",
    "min_diff": 5,
    "undercut_amount": 2,
    "min_match_score": 95,
    "max_loss_pct": 10,
})
        """, language="python")

    # ── تاب 3: البحث الدوري ──
    with tab_a3:
        st.subheader("🔍 البحث الدوري عن أسعار المنافسين")

        c1, c2 = st.columns(2)
        c1.metric("⏱️ البحث القادم", search_mgr.time_until_next())
        c2.metric("📊 آخر نتائج", f"{len(search_mgr.last_results)} منتج")

        if st.session_state.analysis_df is not None:
            scan_count = st.slider("عدد المنتجات للمسح", 5, 50, 15, key="scan_n")
            if st.button("🔍 مسح السوق الآن", type="primary", key="scan_now"):
                with st.spinner(f"يبحث عن أسعار {scan_count} منتج في السوق..."):
                    scan_results = search_mgr.run_scan(st.session_state.analysis_df, scan_count)
                if scan_results:
                    st.success(f"✅ تم مسح {len(scan_results)} منتج بنجاح")
                    for sr in scan_results[:10]:
                        md = sr.get("market_data", {})
                        rec = md.get("recommendation", md.get("market_price", "—"))
                        st.markdown(f"**{sr['product']}** — سعرنا: {sr['our_price']:.0f} | السوق: {rec}")
                else:
                    st.warning("لم يتم العثور على نتائج — تحقق من اتصال AI")
        else:
            st.warning("ارفع ملفات التحليل أولاً")

    # ── تاب 4: سجل القرارات ──
    with tab_a4:
        st.subheader("📊 سجل قرارات الأتمتة")
        days_filter = st.selectbox("الفترة", [7, 14, 30], index=0, key="auto_log_days")

        stats = get_automation_stats(days_filter)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("إجمالي", stats["total"])
        c2.metric("خفض", stats["lower"])
        c3.metric("رفع", stats["raise"])
        c4.metric("أُرسل لـ Make", stats["pushed"])

        log_data = get_automation_log(limit=100)
        if log_data:
            log_df = pd.DataFrame(log_data)
            display = ["timestamp", "product_name", "action", "old_price",
                        "new_price", "competitor", "match_score", "pushed_to_make"]
            available = [c for c in display if c in log_df.columns]
            st.dataframe(log_df[available].rename(columns={
                "timestamp": "التاريخ", "product_name": "المنتج",
                "action": "الإجراء", "old_price": "السعر القديم",
                "new_price": "السعر الجديد", "competitor": "المنافس",
                "match_score": "التطابق%", "pushed_to_make": "أُرسل؟"
            }), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة بعد — شغّل الأتمتة من التاب الأول")
