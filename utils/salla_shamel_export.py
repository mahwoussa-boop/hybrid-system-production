"""
utils/salla_shamel_export.py — تصدير سلة الشامل (Production-Ready v2.0)
═══════════════════════════════════════════════════════════════════════
• رؤوس أعمدة مطابقة حرفياً لقالب سلة 2024
• ترميز UTF-8 مع BOM (utf-8-sig) — Excel وسلة يتعرفان على العربية
• مطابقة التصنيفات والماركات من ملفات CSV الرسمية (fuzzy match)
• لا أعمدة زائدة — أي حقل خارج القالب يُرفض من سلة
"""
import csv
import functools
import html
import io
import logging
import os
import re
from typing import Optional

import pandas as pd

from utils.helpers import safe_float

_logger = logging.getLogger(__name__)

_HTML_TAG_RE = re.compile(r"<[^>]+>")

# هامش الربح الافتراضي فوق سعر المنافس (قابل للتغيير بـ env var)
_PRICE_MARKUP_FACTOR = float(os.environ.get("SALLA_PRICE_MARKUP", "1.05"))

# ══════════════════════════════════════════════════════════════════════════════
#  قالب سلة 2024 — الأعمدة بالترتيب والمسمى الحرفي
#  تنبيه: "النوع " (مسافة في النهاية) و"أسم المنتج" (همزة قطع) — لا تغيّرهما
#  "الكمية المتوفرة" محذوف — غير موجود في القالب الجديد لعام 2024
# ══════════════════════════════════════════════════════════════════════════════
SALLA_SHAMEL_COLUMNS = [
    "النوع ",                           # ← مسافة في النهاية (هكذا في قالب سلة)
    "أسم المنتج",                       # ← همزة قطع — لا تبدّلها بـ "اسم"
    "تصنيف المنتج",
    "صورة المنتج",
    "وصف صورة المنتج",
    "نوع المنتج",
    "سعر المنتج",
    "الوصف",
    "هل يتطلب شحن؟",
    "رمز المنتج sku",
    "سعر التكلفة",
    "السعر المخفض",
    "تاريخ بداية التخفيض",
    "تاريخ نهاية التخفيض",
    "اقصي كمية لكل عميل",
    "إخفاء خيار تحديد الكمية",
    "اضافة صورة عند الطلب",
    "الوزن",
    "وحدة الوزن",
    "الماركة",
    "العنوان الترويجي",
    "تثبيت المنتج",
    "الباركود",
    "السعرات الحرارية",
    "MPN",
    "GTIN",
    "خاضع للضريبة ؟",
    "سبب عدم الخضوع للضريبة",
    "[1] الاسم",
    "[1] النوع",
    "[1] القيمة",
    "[1] الصورة / اللون",
    "[2] الاسم",
    "[2] النوع",
    "[2] القيمة",
    "[2] الصورة / اللون",
    "[3] الاسم",
    "[3] النوع",
    "[3] القيمة",
    "[3] الصورة / اللون",
]

# ══════════════════════════════════════════════════════════════════════════════
#  تحميل بيانات المرجع (مرة واحدة عند أول استدعاء — @functools.lru_cache)
# ══════════════════════════════════════════════════════════════════════════════

def _catalog_csv_path(filename: str) -> str:
    """مسار ملف الكتالوج: DATA_DIR أولاً ثم data/ المحلية."""
    data_dir = (os.environ.get("DATA_DIR") or "").strip()
    if data_dir:
        p = os.path.join(data_dir, filename)
        if os.path.exists(p):
            return p
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(root, "data", filename)


@functools.lru_cache(maxsize=1)
def _load_valid_categories() -> list:
    """يقرأ قائمة التصنيفات المعتمدة من categories.csv — مُخزَّنة في الذاكرة."""
    for fname in ("تصنيفات مهووس.csv", "categories.csv"):
        path = _catalog_csv_path(fname)
        if not os.path.exists(path):
            continue
        for enc in ("utf-8-sig", "cp1256", "utf-8"):
            try:
                df = pd.read_csv(path, encoding=enc)
                col = df.columns[0]
                vals = [
                    str(v).strip().strip('"').lstrip('\ufeff').strip()
                    for v in df[col].dropna().tolist()
                ]
                vals = [v for v in vals if v and v not in ("nan", "none")]
                if vals:
                    _logger.info("تحميل %d تصنيف من %s (%s)", len(vals), path, enc)
                    return vals
            except Exception:
                continue
    _logger.warning("_load_valid_categories: لم يُعثر على ملف تصنيفات صالح")
    return []


@functools.lru_cache(maxsize=1)
def _load_valid_brands() -> list:
    """يقرأ قائمة الماركات المعتمدة من brands.csv — مُخزَّنة في الذاكرة."""
    for fname in ("ماركات مهووس.csv", "brands.csv"):
        path = _catalog_csv_path(fname)
        if not os.path.exists(path):
            continue
        for enc in ("utf-8-sig", "cp1256", "utf-8"):
            try:
                df = pd.read_csv(path, encoding=enc)
                col = df.columns[0]
                vals = [
                    str(v).strip().strip('"').lstrip('\ufeff').strip()
                    for v in df[col].dropna().tolist()
                ]
                vals = [v for v in vals if v and v not in ("nan", "none")]
                if vals:
                    _logger.info("تحميل %d ماركة من %s (%s)", len(vals), path, enc)
                    return vals
            except Exception:
                continue
    _logger.warning("_load_valid_brands: لم يُعثر على ملف ماركات صالح")
    return []


# ══════════════════════════════════════════════════════════════════════════════
#  منطق المطابقة الذكية للتصنيفات
# ══════════════════════════════════════════════════════════════════════════════

# خريطة حتمية: (جنس، نوع_المنتج_lowercase) → كلمة بحث
_CAT_RULE_MAP = [
    # --- شعر ---
    (None,      "hair_mist",    "عطور الشعر"),
    (None,      "hair mist",    "عطور الشعر"),
    (None,      "شعر",          "عطور الشعر"),
    # --- جسم ---
    (None,      "body_mist",    "عطور الجسم"),
    (None,      "body mist",    "عطور الجسم"),
    (None,      "جسم",          "عطور الجسم"),
    (None,      "بودي",         "عطور الجسم"),
    # --- تستر ---
    (None,      "تستر",         "عطور التستر"),
    (None,      "tester",       "عطور التستر"),
    # --- نيش ---
    (None,      "نيش",          "عطور النيش"),
    (None,      "niche",        "عطور النيش"),
    # --- بدائل ---
    (None,      "بديل",         "بدائل العطور"),
    (None,      "dupe",         "بدائل العطور"),
    # --- فرمونية ---
    (None,      "فرمون",        "عطور فرمونية"),
    (None,      "pheromon",     "عطور فرمونية"),
    # --- أطفال ---
    (None,      "أطفال",        "عطور الأطفال"),
    (None,      "اطفال",        "عطور الأطفال"),
    (None,      "kids",         "عطور الأطفال"),
    (None,      "children",     "عطور الأطفال"),
    # --- بيونا جنس ---
    ("رجالي",   "",             "عطور رجالية"),
    ("رجال",    "",             "عطور رجالية"),
    ("للرجال",  "",             "عطور رجالية"),
    ("men",     "",             "عطور رجالية"),
    ("homme",   "",             "عطور رجالية"),
    ("male",    "",             "عطور رجالية"),
    ("نسائي",   "",             "عطور نسائية"),
    ("نساء",    "",             "عطور نسائية"),
    ("للنساء",  "",             "عطور نسائية"),
    ("women",   "",             "عطور نسائية"),
    ("femme",   "",             "عطور نسائية"),
    ("female",  "",             "عطور نسائية"),
]

_CAT_FALLBACK = "العطور"


def _best_category_from_rules(product_name: str, gender: str, ptype: str) -> str:
    """
    يطابق حتمياً أولاً، ثم بـ rapidfuzz.
    يُرجع دائماً قيمة صالحة (لا None).
    """
    valid = _load_valid_categories()
    s_name   = str(product_name or "").lower()
    s_gender = str(gender  or "").lower()
    s_type   = str(ptype   or "").lower()
    combined = f"{s_name} {s_gender} {s_type}"

    # ── مرحلة 1: قواعد حتمية ──────────────────────────────────────────────
    for gender_kw, type_kw, search_term in _CAT_RULE_MAP:
        if gender_kw and gender_kw.lower() not in combined:
            continue
        if type_kw and type_kw.lower() not in combined:
            continue
        # ابحث عن search_term في قائمة التصنيفات الفعلية
        if not valid:
            return search_term  # لا توجد قائمة — أعد الكلمة مباشرةً
        # مطابقة مباشرة أولاً
        for v in valid:
            if v == search_term or v.startswith(search_term):
                return v
        # ثم fuzzy
        try:
            from rapidfuzz import process as rf_proc, fuzz as rf_fuzz
            hit = rf_proc.extractOne(search_term, valid, scorer=rf_fuzz.token_set_ratio)
            if hit and hit[1] >= 55:
                return hit[0]
        except ImportError:
            pass
        return search_term  # fallback على الكلمة مباشرةً

    # ── مرحلة 2: fuzzy مفتوح على الاسم كاملاً ─────────────────────────────
    if valid:
        try:
            from rapidfuzz import process as rf_proc, fuzz as rf_fuzz
            hit = rf_proc.extractOne(combined, valid, scorer=rf_fuzz.token_set_ratio)
            if hit and hit[1] >= 45:
                return hit[0]
        except ImportError:
            pass
        # الفئة الأم الافتراضية
        for v in valid:
            if v == _CAT_FALLBACK:
                return v
        return valid[0]

    return _CAT_FALLBACK  # Hard Rule: لا تُرجع فارغاً أبداً


def _best_brand_from_csv(raw_brand: str) -> str:
    """
    يُرجع الاسم الحرفي للماركة من brands.csv (نسخ حرفي).
    إذا لم تُوجد مطابقة ≥ 75% → يُعيد raw_brand كما هو.
    Hard Rule: لا تُرجع فارغاً أبداً — القيمة الافتراضية "غير محدد".
    """
    raw = str(raw_brand or "").strip()
    if not raw or raw.lower() in ("nan", "none", ""):
        return "غير محدد"          # ← Hard Rule
    valid = _load_valid_brands()
    if not valid:
        return raw
    # مطابقة مباشرة أولاً
    for v in valid:
        if raw.lower() == v.lower():
            return v
        # المقارنة بعد الـ | (الاسم الإنجليزي)
        parts = [p.strip() for p in v.split("|")]
        if any(raw.lower() == p.lower() for p in parts):
            return v
    # ثم fuzzy
    try:
        from rapidfuzz import process as rf_proc, fuzz as rf_fuzz
        hit = rf_proc.extractOne(raw, valid, scorer=rf_fuzz.token_set_ratio)
        if hit and hit[1] >= 75:
            return hit[0]
    except ImportError:
        pass
    return raw  # لا مطابقة — أبقِ الاسم الأصلي


# ══════════════════════════════════════════════════════════════════════════════
#  دوال تنظيف البيانات
# ══════════════════════════════════════════════════════════════════════════════

def _strip_html_visible(s: str) -> str:
    if not s:
        return ""
    t = _HTML_TAG_RE.sub(" ", str(s))
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def _is_url(s: str) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _single_image_url(raw: str) -> str:
    """
    يُرجع رابط صورة واحد نظيف — لا فراغات، لا روابط متعددة.
    يدعم روابط CDN سلة التي تحتوي فواصل في معاملات التحويل
    مثل: cdn.salla.sa/cdn-cgi/image/width=800,fit=contain/...
    """
    s = str(raw or "").strip()
    if not s:
        return ""
    if s.startswith("data:"):
        return ""
    # قطع عند بداية أي رابط ثانٍ (https:// أو http:// بعد الحرف 8)
    idx_http  = s.find("http://", 8)
    idx_https = s.find("https://", 8)
    cuts = [i for i in (idx_http, idx_https) if i > 0]
    if cuts:
        s = s[:min(cuts)].rstrip(" \t\n\r")
    # محاولة 1: رابط ينتهي بامتداد صريح (يشمل CDN الذي ينتهي بامتداد)
    m = re.search(
        r"(https?://[^\s<>\"'؛;]+?\.(?:webp|jpg|jpeg|png|gif|avif)(?:[?#][^\s<>\"']*)?)",
        s, re.I,
    )
    if m:
        return m.group(1).rstrip(".,;)>]")
    # محاولة 2: رابط CDN سلة — يحتوي /cdn-cgi/ أو /image/ مع فواصل في المعاملات
    m_cdn = re.search(
        r"(https?://[^\s\"'<>]+/(?:cdn-cgi|image)/[^\s\"'<>]*)",
        s, re.I,
    )
    if m_cdn:
        return m_cdn.group(1).rstrip(".,;)>]")
    # محاولة 3: أي رابط HTTP — الفاصلة مسموحة (CDN) لكن المسافات والاقتباسات لا
    m2 = re.search(r"(https?://[^\s\"'<>\u060c]+)", s)
    return m2.group(1).rstrip(".,;)>]") if m2 else ""


def _plain_name(r: dict) -> str:
    """اسم منتج نصي — لا HTML ولا رابط خام."""
    def _clean(v):
        x = _strip_html_visible(str(v or "").strip())
        return "" if (not x or x.lower() in ("nan", "none", "<na>")) else x

    for key in ("المنتج", "اسم المنتج", "اسم_المنتج", "منتج_المنافس",
                "Product", "Name", "name", "title", "الاسم"):
        v = _clean(r.get(key))
        if v and not _is_url(v):
            return v
    # بناء من الحقول الوصفية
    chunks = [c for c in (_clean(r.get("الماركة")), _clean(r.get("الحجم")), _clean(r.get("النوع"))) if c]
    return " · ".join(chunks) if chunks else ""


def _extract_price(r: dict) -> float:
    for k in ("سعر_المنافس", "سعر المنافس", "السعر", "سعر المنتج", "Price", "price"):
        p = safe_float(r.get(k), 0.0)
        if p > 0:
            return round(p, 2)
    return 0.0


def _extract_sku(r: dict) -> str:
    """
    يستخرج SKU من الصف ويُنظّفه عبر sanitize_sku.
    يُمنع وضع روابط URL مباشرةً — يُحوَّل الرابط لرمز فريد.
    """
    from utils.data_helpers import sanitize_sku as _sanitize_sku

    for k in ("معرف_المنافس", "رمز المنتج sku", "رمز_المنتج_sku",
              "SKU", "sku", "رمز المنتج", "رقم المنتج", "Barcode", "barcode"):
        v = r.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "<na>"):
            continue
        # sanitize_sku يتعامل مع الرابط والرقم والنص
        return _sanitize_sku(s)

    # إذا لم يُوجد حقل SKU → حاول الاشتقاق من رابط المنتج
    for url_k in ("رابط_المنافس", "رابط المنتج", "product_url", "url"):
        u = str(r.get(url_k) or "").strip()
        if u.startswith("http"):
            return _sanitize_sku(u)

    return ""


def _extract_weight(r: dict) -> tuple:
    w = safe_float(r.get("الوزن"), 0.0)
    unit = str(r.get("وحدة الوزن") or r.get("weight_unit") or "").strip()
    return (round(max(w, 0.2), 4), unit or "kg")


def _placeholder_description(name: str, brand: str) -> str:
    return (
        f"عطر أصلي 100% — {name}{' — ' + brand if brand else ''}. "
        f"متوفر لدى متجر مهووس للعطور. "
        f"استخدم زر «خبير الوصف» في صفحة المفقودات لتوليد وصف تسويقي كامل."
    )


# ── أحرف خاصة يرفضها حقل «وصف صورة المنتج» في سلة ─────────────────────────
_ALT_SPECIAL_RE = re.compile(r"[^\w\s\u0600-\u06FF\u0750-\u077F\u08A0-\u08FF]", re.UNICODE)


def _safe_alt_text(name: str, max_len: int = 125) -> str:
    """
    يُنتج نص alt مقبول من سلة:
    - يحذف الأحرف الخاصة (| ( ) ! @ # & — – … إلخ).
    - يحتفظ بالحروف العربية والإنجليزية والأرقام والمسافات فقط.
    - يحذف المسافات المتعددة.
    - يقطع عند max_len.
    """
    s = _strip_html_visible(str(name or ""))
    s = _ALT_SPECIAL_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:max_len].strip()


def _safe_promo(name: str, brand: str, max_len: int = 25) -> str:
    """
    يُنتج العنوان الترويجي بحد أقصى 25 حرفاً (قيد سلة).
    يُفضّل اسم المنتج المختصر على دمجه مع الماركة إذا كان الدمج يتجاوز الحد.
    """
    # محاولة دمج الاسم + الماركة
    brand_clean = str(brand or "").strip()
    if brand_clean and "|" in brand_clean:
        # استخدم الجزء الإنجليزي من الاسم الثنائي كاختصار
        parts = [p.strip() for p in brand_clean.split("|")]
        brand_short = next((p for p in parts if re.search(r"[a-zA-Z]", p)), parts[0])
    else:
        brand_short = brand_clean

    full = f"{name} {brand_short}".strip() if brand_short else name
    if len(full) <= max_len:
        return full

    # اقطع اسم المنتج فقط عند max_len (كلمة كاملة)
    short = name[:max_len]
    if " " in short:
        short = short.rsplit(" ", 1)[0]
    return short.rstrip(".,،؛—–").strip()[:max_len]


# ══════════════════════════════════════════════════════════════════════════════
#  الدالة الرئيسية — التصدير
# ══════════════════════════════════════════════════════════════════════════════

def export_to_salla_shamel(
    missing_df: pd.DataFrame,
    generate_descriptions: bool = False,
) -> bytes:
    """
    يُنشئ ملف CSV جاهزاً لاستيراد سلة الشامل.

    الهيكل:
        الصف 1 : «بيانات المنتج» + أعمدة فارغة (مطلوب من سلة)
        الصف 2 : رؤوس الأعمدة (SALLA_SHAMEL_COLUMNS)
        الصف 3+ : البيانات

    الترميز: UTF-8 مع BOM ← Excel وسلة يقرأانه صحيحاً.
    """
    ncols = len(SALLA_SHAMEL_COLUMNS)
    buf   = io.StringIO(newline="")
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)

    # صف الترويسة الذي تطلبه سلة
    writer.writerow(["بيانات المنتج"] + [""] * (ncols - 1))
    writer.writerow(SALLA_SHAMEL_COLUMNS)

    if missing_df is None or missing_df.empty:
        return ("\ufeff" + buf.getvalue()).encode("utf-8")

    # F2 Fix: فلترة المنتجات المؤكدة فقط — تستثني «⚠️ مكرر محتمل»
    if "حالة_المنتج" in missing_df.columns:
        _confirmed = missing_df[
            missing_df["حالة_المنتج"].str.startswith("✅", na=False)
        ]
        if not _confirmed.empty:
            missing_df = _confirmed

    for idx, row in missing_df.iterrows():
        r = row.to_dict()

        # ── الاسم ─────────────────────────────────────────────────────────
        pname = _plain_name(r)
        if not pname:
            pname = _strip_html_visible(str(r.get("منتج_المنافس", "") or "")) or "منتج"

        # ── السعر — سعرنا المقترح = سعر المنافس + هامش ربح افتراضي 5% ──────
        comp_price = _extract_price(r)
        list_price = round(comp_price * _PRICE_MARKUP_FACTOR, 2) if comp_price > 0 else 1.0

        # ── البيانات الوصفية ───────────────────────────────────────────────
        brand  = str(r.get("الماركة",  "") or "").strip()
        gender = str(r.get("الجنس",    "") or "").strip()
        ptype  = str(r.get("النوع",    "") or "").strip()
        sku    = _extract_sku(r)
        w_val, w_unit = _extract_weight(r)

        # ── الصورة — رابط واحد نظيف ────────────────────────────────────────
        raw_img = str(r.get("صورة_المنافس", "") or r.get("image_url", "") or "").strip()
        img     = _single_image_url(raw_img)

        # ── التصنيف — من ملف سلة الرسمي ─────────────────────────────────
        # أولوية: عمود "تصنيف_سلة_الدقيق" (إن وُلِّد مسبقاً) ثم الاستنتاج الذكي
        preset_cat = str(r.get("تصنيف_سلة_الدقيق", "") or "").strip()
        if preset_cat and preset_cat not in ("nan", "none"):
            category = preset_cat
        else:
            category = _best_category_from_rules(pname, gender, ptype)

        # ── الماركة — BrandManager (fuzzy + كشف الجديدة) ─────────────────
        preset_brand = str(r.get("الماركة_المعتمدة", "") or "").strip()
        if preset_brand and preset_brand not in ("nan", "none"):
            brand_out = preset_brand
        else:
            raw_brand_val = brand if brand else ""
            if raw_brand_val:
                try:
                    from utils.brand_manager import resolve_brand
                    brand_out, _is_new = resolve_brand(raw_brand_val, auto_generate=True)
                    if _is_new:
                        _logger.info(
                            "salla_shamel_export: ماركة جديدة مكتشفة — «%s» → «%s»",
                            raw_brand_val, brand_out,
                        )
                except Exception as _bm_err:
                    _logger.warning("BrandManager fallback: %s", _bm_err)
                    brand_out = _best_brand_from_csv(raw_brand_val)
            else:
                brand_out = ""

        # ── الوصف — أولوية: الوصف_الآلي المولد مسبقاً ثم generate_descriptions ثم placeholder ──
        _ai_desc = _strip_html_visible(str(r.get("الوصف_الآلي", "") or "")).strip()
        if _ai_desc and _ai_desc.lower() not in ("nan", "none"):
            desc_text = _ai_desc
        elif generate_descriptions:
            try:
                from engines.ai_engine import generate_salla_html_description
                raw_scraped = str(r.get("raw_description", "") or "").strip()
                desc_text = generate_salla_html_description(pname, raw_scraped)
            except Exception as _e:
                _logger.warning("generate_salla_html_description فشل للمنتج '%s': %s", pname, _e)
                desc_text = "عطر أصلي من متجر مهووس"
        else:
            desc_text = "عطر أصلي من متجر مهووس"

        # وصف الصورة — نص خالٍ من الأحرف الخاصة (قيد سلة)
        alt_txt = _safe_alt_text(f"زجاجة عطر {pname} الأصلية")

        # العنوان الترويجي — حد أقصى 25 حرفاً (قيد سلة الصارم)
        promo = _safe_promo(pname, brand_out)

        # ── بناء الصف بالترتيب الحرفي للأعمدة ────────────────────────────
        row_map = {
            "النوع ":                        "منتج",        # ← مسافة في المفتاح
            "أسم المنتج":                    pname,
            "تصنيف المنتج":                  category,
            "صورة المنتج":                   img,
            "وصف صورة المنتج":               alt_txt,
            "نوع المنتج":                    "منتج جاهز",   # ← ثابت
            "سعر المنتج":                    list_price,
            "الوصف":                         desc_text,
            "هل يتطلب شحن؟":                "نعم",          # ← ثابت
            "رمز المنتج sku":                sku,
            "سعر التكلفة":                   "",
            "السعر المخفض":                  "",
            "تاريخ بداية التخفيض":           "",
            "تاريخ نهاية التخفيض":           "",
            "اقصي كمية لكل عميل":           "",
            "إخفاء خيار تحديد الكمية":      "",
            "اضافة صورة عند الطلب":         "",
            "الوزن":                         w_val,
            "وحدة الوزن":                    w_unit,
            "الماركة":                       brand_out,
            "العنوان الترويجي":              promo,
            "تثبيت المنتج":                  "",
            "الباركود":                      str(r.get("الباركود") or r.get("Barcode") or "").strip(),
            "السعرات الحرارية":              "",
            "MPN":                           "",
            "GTIN":                          "",
            "خاضع للضريبة ؟":               "نعم",          # ← ثابت
            "سبب عدم الخضوع للضريبة":       "",
            "[1] الاسم":                     "",
            "[1] النوع":                     "",
            "[1] القيمة":                    "",
            "[1] الصورة / اللون":            "",
            "[2] الاسم":                     "",
            "[2] النوع":                     "",
            "[2] القيمة":                    "",
            "[2] الصورة / اللون":            "",
            "[3] الاسم":                     "",
            "[3] النوع":                     "",
            "[3] القيمة":                    "",
            "[3] الصورة / اللون":            "",
        }

        writer.writerow([row_map[col] for col in SALLA_SHAMEL_COLUMNS])

    # UTF-8 مع BOM
    return ("\ufeff" + buf.getvalue()).encode("utf-8")


# ══════════════════════════════════════════════════════════════════════════════
#  دوال مساعدة للواجهة — إدارة الماركات الجديدة
# ══════════════════════════════════════════════════════════════════════════════

def export_new_brands_salla_csv() -> bytes:
    """
    يُصدّر Brands_Salla.csv للماركات الجديدة المكتشفة في هذه الجلسة.
    يجب رفعه على سلة *قبل* ملف المنتجات CSV.
    """
    from utils.brand_manager import export_new_brands_csv
    return export_new_brands_csv()


def get_new_brands_summary() -> dict:
    """
    يُعيد ملخص الماركات الجديدة للعرض في الواجهة.

    Returns:
        {"count": int, "brands": [{"name":..., "seo_url":..., "visual_prompt":...}]}
    """
    from utils.brand_manager import get_new_brands_list, get_visual_prompt
    brands_data = get_new_brands_list()
    return {
        "count": len(brands_data),
        "brands": [
            {
                "name":          bd.get("brand_name", ""),
                "description":   bd.get("description", ""),
                "seo_url":       bd.get("seo_url", ""),
                "seo_title":     bd.get("seo_title", ""),
                "visual_prompt": bd.get("logo_prompt") or get_visual_prompt(
                    bd.get("brand_name", "")
                ),
            }
            for bd in brands_data
        ],
    }


def invalidate_brand_cache() -> None:
    """
    يُفرغ ذاكرة التخزين المؤقت للماركات والأقسام — يُستدعى بعد رفع ملف brands.csv جديد.
    يُصفّر كلاً من LRU cache الموجود ومفتاح BrandManager وـ ai_engine.
    """
    _load_valid_brands.cache_clear()
    _load_valid_categories.cache_clear()
    try:
        from utils.brand_manager import reload_brands_file
        reload_brands_file()
    except Exception:
        pass
    try:
        from engines.ai_engine import clear_catalog_cache
        clear_catalog_cache()
    except Exception:
        pass
