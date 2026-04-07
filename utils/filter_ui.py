"""
utils/filter_ui.py — محرك الفلترة المركزي
═══════════════════════════════════════════
فلاتر سريعة في الشريط الجانبي تُطبَّق عبر جميع أقسام البيانات بالتساوي.
تُكمّل (لا تستبدل) الفلاتر التفصيلية الموجودة داخل كل قسم.
"""
import streamlit as st
import pandas as pd

# ── مفاتيح session_state للفلاتر العالمية (مُصطلح بـ _gf_ للتمييز) ─────────
_GF_BRAND  = "_gf_brand"
_GF_RISK   = "_gf_risk"
_GF_SEARCH = "_gf_search"


def render_sidebar_filters(df: pd.DataFrame) -> None:
    """
    يرسم عناصر الفلترة السريعة في الشريط الجانبي.
    يجب استدعاؤه مرة واحدة فقط في كل تشغيل داخل `with st.sidebar:`.

    الفلاتر المعروضة:
    - الماركة (selectbox ديناميكي من البيانات)
    - مستوى الخطورة (selectbox ديناميكي إذا وُجد العمود)
    - بحث نصي سريع
    """
    if df is None or df.empty:
        return

    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🔍 فلاتر سريعة")

    # فلتر الماركة
    if "الماركة" in df.columns:
        _brands = sorted(
            str(b) for b in df["الماركة"].dropna().unique()
            if str(b).strip() and str(b) not in ("nan", "None")
        )
        if _brands:
            st.sidebar.selectbox("🏷️ الماركة", ["الكل"] + _brands, key=_GF_BRAND)
        else:
            st.session_state.setdefault(_GF_BRAND, "الكل")
    else:
        st.session_state.setdefault(_GF_BRAND, "الكل")

    # فلتر مستوى الخطورة
    if "الخطورة" in df.columns:
        _risks = sorted(
            str(r) for r in df["الخطورة"].dropna().unique()
            if str(r).strip() and str(r) not in ("nan", "None")
        )
        if _risks:
            st.sidebar.selectbox("⚡ الخطورة", ["الكل"] + _risks, key=_GF_RISK)
        else:
            st.session_state.setdefault(_GF_RISK, "الكل")
    else:
        st.session_state.setdefault(_GF_RISK, "الكل")

    # بحث نصي سريع
    st.sidebar.text_input("🔎 بحث سريع", key=_GF_SEARCH, placeholder="اسم أو SKU أو ماركة…")


def apply_global_filters(df: pd.DataFrame) -> pd.DataFrame:
    """
    يطبّق قيم الفلاتر العالمية الحالية من session_state على الـ DataFrame.

    آمن تماماً:
    - يُرجع df كما هو إذا لم تكن الفلاتر نشطة أو الأعمدة غائبة.
    - لا يُعدِّل df الأصلي (يعمل على نسخة).
    """
    if df is None or df.empty:
        return df

    brand_v  = str(st.session_state.get(_GF_BRAND,  "الكل") or "الكل")
    risk_v   = str(st.session_state.get(_GF_RISK,   "الكل") or "الكل")
    search_v = str(st.session_state.get(_GF_SEARCH, "")     or "").strip()

    # مبكرًا: لا فلاتر نشطة → لا تكلفة
    if brand_v == "الكل" and risk_v == "الكل" and not search_v:
        return df

    result = df.copy()

    if brand_v != "الكل" and "الماركة" in result.columns:
        result = result[result["الماركة"].astype(str) == brand_v]

    if risk_v != "الكل" and "الخطورة" in result.columns:
        result = result[result["الخطورة"].astype(str) == risk_v]

    if search_v:
        _mask = pd.Series([False] * len(result), index=result.index)
        for _col in ("المنتج", "معرف_المنتج", "الماركة", "منتج_المنافس"):
            if _col in result.columns:
                _mask = _mask | result[_col].astype(str).str.contains(
                    search_v, case=False, na=False
                )
        result = result[_mask]

    return result.reset_index(drop=True)


def get_active_filter_summary() -> str:
    """
    ملخص نصي للفلاتر العالمية النشطة — للعرض في caption الجداول.
    مثال: "ماركة: Dior | خطورة: 🔴 حرج | بحث: sauvage"
    """
    _parts = []
    brand_v  = str(st.session_state.get(_GF_BRAND,  "الكل") or "الكل")
    risk_v   = str(st.session_state.get(_GF_RISK,   "الكل") or "الكل")
    search_v = str(st.session_state.get(_GF_SEARCH, "")     or "").strip()
    if brand_v  != "الكل": _parts.append(f"ماركة: {brand_v}")
    if risk_v   != "الكل": _parts.append(f"خطورة: {risk_v}")
    if search_v:            _parts.append(f"بحث: {search_v}")
    return " | ".join(_parts) if _parts else ""
