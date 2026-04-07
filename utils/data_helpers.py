"""
دوال مساعدة خالصة (بدون واجهة ولا session_state) — تجهيز البيانات والنصوص.
"""
import json
import os
import re
from datetime import datetime

import pandas as pd
from rapidfuzz import process as rf_proc, fuzz as rf_fuzz

# تسلسل شائع في سلة/إكسيل: ...jpg,https://...
_AFTER_EXT_COMMA_HTTP = re.compile(
    r"\.(?:webp|jpg|jpeg|png|gif|avif)\s*[,،]\s*https?://",
    re.I,
)


def _looks_like_several_image_urls(s: str) -> bool:
    """True فقط عندما يُرجّح أن النص يضم أكثر من رابط (لا نلمس رابط المنافس بفاصلة داخل ?query)."""
    if not s or ("http://" not in s and "https://" not in s):
        return False
    n = s.count("http://") + s.count("https://")
    if n > 1:
        return True
    return bool(_AFTER_EXT_COMMA_HTTP.search(s))

# حقول وسائط قد تُحفظ كـ NaN — لا تُستبدل بالصفر
_MEDIA_KEYS_EMPTY_ON_NA = frozenset({
    "صورة_منتجنا", "رابط_منتجنا", "صورة_المنتج", "رابط_المنتج",
    "رابط_المنافس",
    "صورة المنتج", "رابط المنتج", "صوره المنتج", "الرابط", "رابط",
})


def first_image_url_string(s: str) -> str:
    """
    أرجع أول رابط http يبدو ملف صورة، مع دعم استثنائي لروابط Cloudflare/Salla CDN
    التي تحتوي على فواصل في مسارها (مثل fit=scale-down,width=500).
    """
    s = (s or "").strip()
    if not s: return ""

    # فصل الروابط المتعددة المدمجة بمسافة أو فاصلة دون تدمير روابط CDN
    if "http" in s.lower():
        start = s.lower().find("http")
        next_http = s.lower().find("http", start + 4)
        if next_http > 0:
            s = s[:next_http].rstrip(",، \t\n\r")

    if "cdn-cgi/image" in s or "cdn.salla" in s:
        inner = re.search(r'cdn-cgi/image/[^/]+/(https?://[^\s<>"\']+)', s)
        if inner:
            return inner.group(1).rstrip(".,;)>]")
        m = re.search(r"(https?://[^\s\"\'<>]+)", s)
        return m.group(1).rstrip(".,;)>]") if m else s.split()[0]

    m = re.search(r"(https?://[^\s<>\"\'\,\u060c؛;]+?\.(?:webp|jpg|jpeg|png|gif|avif))", s, re.I)
    if m: return m.group(1).rstrip(".,;)>]")

    m2 = re.search(r"(https?://[^\s\"\'<>]+)", s)
    return m2.group(1).rstrip(".,;)>]") if m2 else s.split()[0]


def _strip_media_val(v):
    if v is None:
        return ""
    try:
        if isinstance(v, float) and pd.isna(v):
            return ""
        if pd.isna(v) and not isinstance(v, (list, dict, str)):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "0", "<na>"):
        return ""
    return s


def normalize_result_media_keys(row: dict) -> None:
    """يوحّد صورة/رابط منتجنا تحت المفتاحين المعتمدين في الواجهة والمحرك."""
    if not row:
        return
    if not _strip_media_val(row.get("صورة_منتجنا")):
        for alt in ("صورة_المنتج", "صورة المنتج", "صوره المنتج"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["صورة_منتجنا"] = v
                    break
    if not _strip_media_val(row.get("رابط_منتجنا")):
        for alt in ("رابط_المنتج", "رابط المنتج", "الرابط", "رابط"):
            if alt in row:
                v = _strip_media_val(row.get(alt))
                if v:
                    row["رابط_منتجنا"] = v
                    break


def row_media_urls_from_analysis(row) -> tuple:
    """
    صورة منتجنا + صورة المنافس الرئيسي من صف نتيجة (Series أو dict).
    يعتمد على مفتاحي صورة_منتجنا وجميع_المنافسين بعد التطبيع.
    """
    if row is None:
        return ("", "")
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    our_img = first_image_url_string(str(d.get("صورة_منتجنا", "") or "").strip())
    comp_img = first_image_url_string(str(d.get("صورة_المنافس", "") or "").strip())
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if not isinstance(all_c, list):
        all_c = []
    comp_name = str(d.get("منتج_المنافس", "—"))
    for c in all_c:
        if str(c.get("name", "")).strip() == str(comp_name).strip():
            # لا نكتب فوق الصورة الصحيحة الموجودة بقيمة فارغة
            candidate_img = first_image_url_string(
                str(c.get("image_url") or c.get("thumb") or c.get("صورة_المنافس") or "").strip()
            )
            if candidate_img:
                comp_img = candidate_img
            break
    # fallback: أول منافس في القائمة إن لم تُوجد صورة بعد
    if not comp_img and all_c:
        comp_img = first_image_url_string(
            str(all_c[0].get("image_url") or all_c[0].get("thumb") or "").strip()
        )
    return (our_img, comp_img)


def our_product_url_from_row(row) -> str:
    """رابط صفحة منتجنا — بعد تطبيع أسماء الأعمدة (رابط_منتجنا / رابط_المنتج / …)."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    normalize_result_media_keys(d)
    u = _strip_media_val(d.get("رابط_منتجنا"))
    if not u.startswith("http"):
        return ""
    return u.split()[0]


def competitor_product_url_from_row(row) -> str:
    """رابط صفحة المنتج عند المنافس — أعمدة النتيجة أو جميع_المنافسين أو أسماء مثل abs-size href."""
    if row is None:
        return ""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    for k in ("رابط_المنافس", "رابط المنافس", "competitor_url"):
        v = _strip_media_val(d.get(k))
        if v.startswith("http"):
            return v.split()[0]
    comp_name = str(d.get("منتج_المنافس", "—"))
    all_c = d.get("جميع_المنافسين", d.get("جميع المنافسين", [])) or []
    if isinstance(all_c, str):
        try:
            all_c = json.loads(all_c)
        except Exception:
            all_c = []
    if isinstance(all_c, list):
        for c in all_c:
            if str(c.get("name", "")).strip() == str(comp_name).strip():
                u = str(c.get("product_url") or c.get("url") or "").strip()
                if u.startswith("http"):
                    return u.split()[0]
        if all_c:
            u = str(all_c[0].get("product_url") or all_c[0].get("url") or "").strip()
            if u.startswith("http"):
                return u.split()[0]
    for k, v in d.items():
        sk = str(k).lower()
        if k in ("رابط_منتجنا", "رابط منتجنا") or "منتجنا" in sk:
            continue
        if "صورة" in str(k) and "وصف" not in str(k) and "href" not in sk:
            continue
        if any(x in sk for x in ("href", "رابط", "link", "url")):
            s = _strip_media_val(v)
            if s.startswith("http"):
                return s.split()[0]
    # أحياناً يُخزَّن رابط صفحة المنتج بالخطأ في عمود الاسم (مثل تصدير المنافس)
    vnm = _strip_media_val(d.get("منتج_المنافس"))
    if vnm.startswith("http"):
        return vnm.split()[0]
    return ""


def safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة."""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                try:
                    row[k] = json.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            else:
                try:
                    if v is not None and not isinstance(v, (list, dict)) and pd.isna(v):
                        row[k] = "" if k in _MEDIA_KEYS_EMPTY_ON_NA else 0
                        continue
                except (TypeError, ValueError):
                    pass
                row[k] = v
        safe.append(row)
    return safe


def restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية."""
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        normalize_result_media_keys(row)
        restored.append(row)
    return restored


def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة (HTML)."""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;'
        f'padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'
    )


def decision_badge(action):
    """شارة قرار معلّق (HTML)."""
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed": ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def pid_from_row(row, col):
    """استخراج معرف المنتج من صف pandas بشكل آمن."""
    if not col or col not in row.index:
        return ""
    v = row.get(col, "")
    if v is None or str(v) in ("nan", "None", "", "NaN"):
        return ""
    try:
        fv = float(v)
        if fv == int(fv):
            return str(int(fv))
    except (ValueError, TypeError):
        pass
    return str(v).strip()


def format_missing_for_salla(missing_df: pd.DataFrame) -> pd.DataFrame:
    """تحويل المنتجات المفقودة إلى قالب سلة الشامل المعتمد.

    يُصدّر فقط المنتجات ذات حالة "✅ مفقود مؤكد" لمنع تكرار المنتجات في سلة.
    ملف الـ Raw Data يحتوي على الكل (بما فيها المكرر المحتمل) للمراجعة.
    """
    if missing_df is None or missing_df.empty:
        return pd.DataFrame()

    # فلترة: صادَر لسلة فقط المنتجات المؤكدة (تستثني ⚠️ مكرر محتمل)
    if "حالة_المنتج" in missing_df.columns:
        salla_input = missing_df[
            missing_df["حالة_المنتج"].str.startswith("✅", na=False)
        ].copy()
        if salla_input.empty:
            return pd.DataFrame()
    else:
        salla_input = missing_df  # الملف القديم بدون العمود → يُصدَّر كله

    salla_input = salla_input.reset_index(drop=True)
    n = len(salla_input)
    salla_df = pd.DataFrame(index=salla_input.index)

    def _series_or_blank(col_name: str):
        if col_name in salla_input.columns:
            return salla_input[col_name].fillna("").astype(str)
        return pd.Series([""] * n, index=salla_input.index)

    # 1) بيانات أساسية إجبارية (بعناوين سلة الحرفية)
    salla_df["النوع"] = ["منتج"] * n
    salla_df["أسم المنتج"] = _series_or_blank("منتج_المنافس")
    # تصنيف: يُفضَّل العمود المُطابَق "تصنيف_سلة_الدقيق" ثم فارغ (لا افتراضي أعمى)
    if "تصنيف_سلة_الدقيق" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("تصنيف_سلة_الدقيق")
    elif "القسم" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("القسم")
    elif "التصنيف" in salla_input.columns:
        salla_df["تصنيف المنتج"] = _series_or_blank("التصنيف")
    else:
        salla_df["تصنيف المنتج"] = ""
    salla_df["صورة المنتج"] = _series_or_blank("صورة_المنافس")
    salla_df["وصف صورة المنتج"] = ""
    salla_df["نوع المنتج"] = ["منتج جاهز"] * n
    # سعر المنتج: رقم صريح — سلة ترفض النص ذا الفواصل أو رموز العملة
    from utils.helpers import safe_float as _sf
    salla_df["سعر المنتج"] = _series_or_blank("سعر_المنافس").map(_sf)
    salla_df["الكمية المتوفرة"] = [0] * n
    # الوصف: يُفضَّل الوصف الآلي (HTML من AI)؛ يُعاد لفارغ إذا لم يُولَّد بعد
    salla_df["الوصف"] = _series_or_blank("الوصف_الآلي")
    salla_df["هل يتطلب شحن؟"] = ["نعم"] * n
    salla_df["رمز المنتج sku"] = _series_or_blank("معرف_المنافس")

    # 2) أعمدة مالية/إدارية
    salla_df["سعر التكلفة"] = ""
    salla_df["السعر المخفض"] = ""
    salla_df["تاريخ بداية التخفيض"] = ""
    salla_df["تاريخ نهاية التخفيض"] = ""
    salla_df["اقصي كمية لكل عميل"] = ""
    salla_df["إخفاء خيار تحديد الكمية"] = ""
    salla_df["اضافة صورة عند الطلب"] = ""
    salla_df["الوزن"] = ""
    salla_df["وحدة الوزن"] = ""

    # 3) الماركة/الحالة — يُفضَّل العمود المُطابَق "الماركة_المعتمدة" (نسخ حرفي من سلة)
    salla_df["حالة المنتج"] = ""
    salla_df["الماركة"] = (
        _series_or_blank("الماركة_المعتمدة")
        if "الماركة_المعتمدة" in salla_input.columns
        else _series_or_blank("الماركة")
    )

    # 4) بقية الأعمدة القياسية
    salla_df["العنوان الترويجي"] = ""
    salla_df["تثبيت المنتج"] = ""
    salla_df["الباركود"] = ""
    salla_df["السعرات الحرارية"] = ""
    salla_df["MPN"] = ""
    salla_df["GTIN"] = ""
    salla_df["خاضع للضريبة ؟"] = ["نعم"] * n
    salla_df["سبب عدم الخضوع للضريبة"] = ""

    return salla_df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════════════════════
#  جدار الحماية للمطابقة الصارمة — Strict Match Firewall
#  يُطبَّق قبل قبول أي مطابقة من الذكاء الاصطناعي أو خوارزمية الـ fuzzy
# ══════════════════════════════════════════════════════════════════════════════

# ── الحجم (ml) ────────────────────────────────────────────────────────────────
_SMV_SIZE_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*(?:ml|مل|ML|Ml|cl|fl\.?\s*oz)',
    re.I | re.UNICODE,
)
_SMV_SIZE_TOLERANCE_ML: float = 10.0

# ── التركيز (مُرتَّب من الأقوى إلى الأضعف — الترتيب مهم) ──────────────────
_SMV_CONC_PATTERNS = [
    (re.compile(
        r'\b(?:extrait\s+de\s+parfum|extrait|اكستريه|اكسترايت|اكسترا\s+دو\s+بارفيوم)\b',
        re.I | re.UNICODE), "EXTRAIT"),
    (re.compile(
        r'\beau\s+de\s+parfum\b|\be\.?d\.?p\b'
        r'|أو\s+دو\s+بارفيوم|او\s+دو\s+بارفيوم|\bأو\s+بارفيوم\b|\bاو\s+بارفيوم\b',
        re.I | re.UNICODE), "EDP"),
    (re.compile(
        r'\beau\s+de\s+toilette\b|\be\.?d\.?t\b'
        r'|أو\s+دو\s+تواليت|او\s+دو\s+تواليت|\bتواليت\b',
        re.I | re.UNICODE), "EDT"),
    (re.compile(
        r'\beau\s+de\s+cologne\b|\be\.?d\.?c\b'
        r'|أو\s+دو\s+كولون|او\s+دو\s+كولون|\bكولون\b(?!\s+إنتنس)|\bcologne\b',
        re.I | re.UNICODE), "EDC"),
    (re.compile(
        r'(?<!\bde\s)(?<!\bدو\s)\bparfum\b'
        r'|\bبارفيوم\b(?!\s+كولون)(?!\s+او)(?!\s+أو)'
        r'|\bبارفان\b',
        re.I | re.UNICODE), "PARFUM"),
    (re.compile(r'\belixir\b|اليكسير|الكسير', re.I | re.UNICODE), "ELIXIR"),
    (re.compile(
        r'\bbody\s*mist\b|\bbody\s*spray\b|\bbody\s*lotion\b'
        r'|بادي\s*ميست|بادي\s*سبراي|بودي\s*ميست',
        re.I | re.UNICODE), "BODY_MIST"),
    (re.compile(
        r'\bhair\s*mist\b|\bhair\s*spray\b|\bhair\s*perfume\b'
        r'|هير\s*ميست|هير\s*سبراي',
        re.I | re.UNICODE), "HAIR_MIST"),
]

# أزواج التركيز غير المتوافقة — أي منها = رفض فوري
_SMV_INCOMPATIBLE: frozenset = frozenset([
    frozenset(["EDP",      "EDT"]),
    frozenset(["EDP",      "EDC"]),
    frozenset(["EDP",      "PARFUM"]),
    frozenset(["EDT",      "PARFUM"]),
    frozenset(["EDT",      "EDC"]),
    frozenset(["EXTRAIT",  "EDP"]),
    frozenset(["EXTRAIT",  "EDT"]),
    frozenset(["EXTRAIT",  "EDC"]),
    frozenset(["EXTRAIT",  "PARFUM"]),
    frozenset(["ELIXIR",   "EDP"]),
    frozenset(["ELIXIR",   "EDT"]),
    frozenset(["ELIXIR",   "PARFUM"]),
    frozenset(["BODY_MIST","EDP"]),
    frozenset(["BODY_MIST","EDT"]),
    frozenset(["BODY_MIST","PARFUM"]),
    frozenset(["BODY_MIST","EXTRAIT"]),
    frozenset(["HAIR_MIST","EDP"]),
    frozenset(["HAIR_MIST","EDT"]),
    frozenset(["HAIR_MIST","PARFUM"]),
    frozenset(["HAIR_MIST","EXTRAIT"]),
])

# ── قاعدة الكلمات المحظورة (Blacklist) ──────────────────────────────────────
# أي كلمة من هذه في أحد الطرفين دون الآخر = رفض فوري
# مثال: "بديل ديور سوفاج" ≠ "Dior Sauvage 100ml" → False
# مثال: "Sauvage Tester" ≠ "Sauvage EDP 100ml"   → False
_SMV_BLACKLIST_RE = re.compile(
    r'\b(?:بديل|بدائل|مستوحى|مستوحاة|inspired\s+by|alternative|'
    r'tester|تستر|تسترز|testers)\b',
    re.I | re.UNICODE,
)


def sanitize_sku(url_or_string: str, prefix: str = "MSNG") -> str:
    """
    تنظيف وتوليد رمز SKU آمن لسلة.

    القواعد:
    ─────────────────────────────────────────────────────────────────────────
    • إذا كانت القيمة رابط URL → استخرج الرقم الرقمي من المسار (مثل /products/10023)
      أو أنشئ رمزاً فريداً من هاش الرابط (مثال: MSNG-A3F8).
    • إذا كانت القيمة رقماً → أعد الرقم مباشرةً.
    • إذا كانت القيمة نصاً صالحاً (حروف/أرقام فقط) → أبقِه كما هو.
    • إذا كانت القيمة فارغة → أعد رمزاً فريداً (prefix + hash قصير).

    أمثلة:
        sanitize_sku("https://example.com/products/10023") → "10023"
        sanitize_sku("https://example.com/p/abc-perfume") → "MSNG-A3B9"
        sanitize_sku("12345")                              → "12345"
        sanitize_sku("")                                   → "MSNG-0000"
    ─────────────────────────────────────────────────────────────────────────
    """
    import hashlib as _hashlib
    s = str(url_or_string or "").strip()

    if not s or s.lower() in ("nan", "none", "<na>", "0"):
        # SKU فارغ → رمز عشوائي منضبط
        return f"{prefix}-0000"

    # رابط URL → استخراج الرقم أو هاش
    if s.startswith("http://") or s.startswith("https://"):
        # محاولة استخراج رقم صريح من المسار: /products/12345 أو ?id=12345
        _num_m = re.search(r'(?:/|id=|product_id=|pid=)(\d{3,})', s, re.I)
        if _num_m:
            return _num_m.group(1)
        # لا يوجد رقم → هاش مختصر (4 أحرف hexadecimal)
        _h = _hashlib.md5(s.encode("utf-8")).hexdigest()[:4].upper()
        return f"{prefix}-{_h}"

    # رقم صريح
    try:
        fv = float(s.replace(",", ""))
        if fv == int(fv) and fv > 0:
            return str(int(fv))
    except (ValueError, TypeError):
        pass

    # نص → تنظيف: احتفظ بالأحرف والأرقام والشرطات فقط
    _clean = re.sub(r"[^\w\-]", "", s, flags=re.UNICODE).strip("-_")
    if _clean and len(_clean) >= 2:
        return _clean[:40]

    # fallback
    _h = _hashlib.md5(s.encode("utf-8")).hexdigest()[:4].upper()
    return f"{prefix}-{_h}"


def strict_match_validator(our_product_name: str, competitor_product_name: str) -> bool:
    """
    جدار الحماية للمطابقة الصارمة.
    يُرجع True  إذا اجتازت المطابقة جميع القواعد.
    يُرجع False فوراً عند أي انتهاك (قبل أي استدعاء AI).

    القواعد الثلاث الصارمة:
    ──────────────────────────────────────────────────────────────────────────
    1. قاعدة الحجم (Volume Rule):
       • يستخرج الحجم (مل) من كلا الاسمَين.
       • إذا كان الحجم موجوداً في أحدهما ومفقوداً في الآخر → False (عدم تماثل).
       • إذا كلاهما له حجم لكنهما مختلفان > 10ml → False.
       أمثلة:
         "ديور سوفاج 100مل" vs "Dior Sauvage 50ml"  → False  (100 ≠ 50)
         "شانيل N°5 50مل"   vs "Chanel No5 EDP"     → False  (50 vs بلا حجم)
         "Sauvage 100ml"    vs "Sauvage EDP 100ml"   → True   (تطابق)

    2. قاعدة التركيز (Concentration Rule):
       • يتعرف على: EDP / EDT / EDC / PARFUM / EXTRAIT / ELIXIR / BODY_MIST / HAIR_MIST
       • إذا التركيز مختلف بين المنتجَين ضمن الأزواج غير المتوافقة → False.
       أمثلة:
         "سوفاج أو دو تواليت"   vs "Sauvage Parfum"     → False (EDT ≠ PARFUM)
         "لانكوم EDP"           vs "Lancome بارفيوم"    → False (EDP ≠ PARFUM)
         "Bleu de Chanel EDP"   vs "بلو دو شانيل EDP"  → True  (متطابق)
         "Sauvage" (بلا تركيز) vs "Sauvage Parfum"    → True  (AI يحكم)

    3. قاعدة الكلمات المحظورة (Blacklist Rule):
       • إذا أحدهما يحتوي: بديل | مستوحى | tester | تستر | inspired by
         والآخر لا يحتوي عليها → False.
       • منتج أصلي لا يطابق بديلاً أو تستراً أبداً.
       أمثلة:
         "ديور سوفاج 100مل"   vs "بديل ديور سوفاج"  → False (blacklist)
         "Dior Sauvage 100ml"  vs "Sauvage Tester"    → False (blacklist)
         "Tester Sauvage EDT"  vs "Tester Sauvage EDP"→ False (EDT≠EDP → قاعدة 2)
         "Tester A"            vs "Tester B"           → True  (كلاهما تستر — AI يحكم)
    ──────────────────────────────────────────────────────────────────────────
    """
    n1 = str(our_product_name or "").strip()
    n2 = str(competitor_product_name or "").strip()
    if not n1 or not n2:
        return False

    # ── قاعدة 3: الكلمات المحظورة (فحص مبكر — الأسرع) ─────────────────────
    bl1 = bool(_SMV_BLACKLIST_RE.search(n1))
    bl2 = bool(_SMV_BLACKLIST_RE.search(n2))
    if bl1 != bl2:
        return False  # أحدهما بديل/تستر والآخر أصلي → رفض قاطع

    # ── قاعدة 1: الحجم ──────────────────────────────────────────────────────
    m1 = _SMV_SIZE_RE.search(n1)
    m2 = _SMV_SIZE_RE.search(n2)
    size1 = float(m1.group(1)) if m1 else None
    size2 = float(m2.group(1)) if m2 else None

    # عدم تماثل: أحدهما له حجم والآخر لا → رفض
    if (size1 is None) != (size2 is None):
        return False

    # كلاهما به حجم → تحقق من الفارق
    if size1 is not None and size2 is not None:
        if abs(size1 - size2) > _SMV_SIZE_TOLERANCE_ML:
            return False

    # ── قاعدة 2: التركيز ────────────────────────────────────────────────────
    conc1 = conc2 = None
    for pattern, label in _SMV_CONC_PATTERNS:
        if conc1 is None and pattern.search(n1):
            conc1 = label
        if conc2 is None and pattern.search(n2):
            conc2 = label
        if conc1 and conc2:
            break

    if conc1 and conc2 and conc1 != conc2:
        if frozenset([conc1, conc2]) in _SMV_INCOMPATIBLE:
            return False

    return True


def strict_match_rejection_reason(our_product_name: str, competitor_product_name: str) -> str:
    """
    يُعيد سبب رفض المطابقة كنص قابل للعرض.
    يُعيد "" إذا اجتازت المطابقة جميع القواعد.
    """
    n1 = str(our_product_name or "").strip()
    n2 = str(competitor_product_name or "").strip()
    if not n1 or not n2:
        return "اسم منتج فارغ"

    bl1 = bool(_SMV_BLACKLIST_RE.search(n1))
    bl2 = bool(_SMV_BLACKLIST_RE.search(n2))
    if bl1 != bl2:
        tag = "بديل/مستوحى" if (bl1 or bl2) else "تستر"
        return f"[Blacklist] أحد المنتجين يحتوي على كلمة محظورة ({tag}) والآخر لا — لا يجوز المطابقة"

    m1 = _SMV_SIZE_RE.search(n1)
    m2 = _SMV_SIZE_RE.search(n2)
    size1 = float(m1.group(1)) if m1 else None
    size2 = float(m2.group(1)) if m2 else None

    if (size1 is None) != (size2 is None):
        has = f"{size1 or size2:.0f}ml"
        return f"[Volume Asymmetry] حجم موجود ({has}) في أحدهما ومفقود في الآخر — مطابقة 0%"

    if size1 is not None and size2 is not None:
        diff = abs(size1 - size2)
        if diff > _SMV_SIZE_TOLERANCE_ML:
            return (
                f"[Volume Mismatch] {size1:.0f}ml ≠ {size2:.0f}ml "
                f"(فارق {diff:.0f}ml > {_SMV_SIZE_TOLERANCE_ML:.0f}ml) — مطابقة 0%"
            )

    conc1 = conc2 = None
    for pattern, label in _SMV_CONC_PATTERNS:
        if conc1 is None and pattern.search(n1):
            conc1 = label
        if conc2 is None and pattern.search(n2):
            conc2 = label
        if conc1 and conc2:
            break

    if conc1 and conc2 and conc1 != conc2:
        if frozenset([conc1, conc2]) in _SMV_INCOMPATIBLE:
            return f"[Concentration Mismatch] {conc1} ≠ {conc2} — مطابقة مستحيلة"

    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  طبقة مطابقة سلة — Salla Validation Layer
# ══════════════════════════════════════════════════════════════════════════════

def _load_categories_list(path: str) -> list:
    """
    يقرأ ملف التصنيفات بالترميزات المدعومة.
    يُنظّف BOM والإقتباسات ويُرجع قائمة نصوص نظيفة.
    """
    for enc in ("cp1256", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            col = df.columns[0]
            vals = [
                str(v).strip().strip('"').lstrip('\ufeff').strip()
                for v in df[col].dropna().tolist()
            ]
            vals = [v for v in vals if v and v.lower() not in ("nan", "none", "")]
            if vals:
                return vals
        except Exception:
            continue
    return []


def _load_brands_list(path: str) -> list:
    """يقرأ ملف الماركات بالترميزات المدعومة."""
    for enc in ("cp1256", "utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=enc)
            col = df.columns[0]
            vals = [
                str(v).strip().strip('"').lstrip('\ufeff').strip()
                for v in df[col].dropna().tolist()
            ]
            vals = [v for v in vals if v and v.lower() not in ("nan", "none", "")]
            if vals:
                return vals
        except Exception:
            continue
    return []


def map_salla_categories(
    missing_df: pd.DataFrame,
    categories_csv_path: str = "",
) -> pd.DataFrame:
    """
    يُعيّن مسار التصنيف الدقيق في سلة بناءً على (النوع) و(الجنس) لكل صف.
    يُضيف عمود "تصنيف_سلة_الدقيق".
    القيمة لا تكون أبداً None — يُستخدم "العطور" كقيمة احتياطية.
    """
    if missing_df is None or missing_df.empty:
        return missing_df

    from utils.data_paths import get_catalog_data_path
    if not categories_csv_path:
        for fname in ("تصنيفات مهووس.csv", "categories.csv"):
            p = get_catalog_data_path(fname)
            if os.path.exists(p):
                categories_csv_path = p
                break

    valid_categories: list = (
        _load_categories_list(categories_csv_path) if categories_csv_path else []
    )

    # خريطة حتمية: (gender_kw, type_kw) → كلمة بحث في قائمة التصنيفات
    _SEARCH_MAP = {
        ("رجالي",   "hair_mist"):  "عطور الشعر",
        ("نسائي",   "hair_mist"):  "عطور الشعر",
        ("للجنسين", "hair_mist"):  "عطور الشعر",
        ("",        "hair_mist"):  "عطور الشعر",
        ("رجالي",   "body_mist"):  "عطور الجسم",
        ("نسائي",   "body_mist"):  "عطور الجسم",
        ("",        "body_mist"):  "عطور الجسم",
        ("رجالي",   "tester"):     "عطور التستر",
        ("نسائي",   "tester"):     "عطور التستر",
        ("",        "tester"):     "عطور التستر",
        ("رجالي",   ""):           "عطور رجالية",
        ("نسائي",   ""):           "عطور نسائية",
        ("للجنسين", ""):           "العطور",
        ("",        ""):           "العطور",
    }
    _FALLBACK_CAT = "العطور"

    def _get_best_category(row) -> str:
        gender = str(row.get("الجنس", "") or "").strip()
        type_  = str(row.get("النوع", "") or "").strip().lower()
        search_term = (
            _SEARCH_MAP.get((gender, type_))
            or _SEARCH_MAP.get(("", type_))
            or _SEARCH_MAP.get((gender, ""))
            or _FALLBACK_CAT
        )
        if not valid_categories:
            return search_term
        # مطابقة مباشرة أولاً
        for v in valid_categories:
            if v == search_term or v.startswith(search_term):
                return v
        # fuzzy
        hit = rf_proc.extractOne(search_term, valid_categories, scorer=rf_fuzz.token_set_ratio)
        if hit and hit[1] >= 55:
            return hit[0]
        # fallback: أول تصنيف عام
        for v in valid_categories:
            if v == _FALLBACK_CAT:
                return v
        return valid_categories[0]

    out = missing_df.copy()
    out["تصنيف_سلة_الدقيق"] = out.apply(_get_best_category, axis=1)
    return out


def validate_salla_brands(
    missing_df: pd.DataFrame,
    brands_csv_path: str = "",
) -> tuple:
    """
    يُطابق ماركة كل منتج مع قائمة الماركات المعتمدة في سلة (نسخ حرفي).

    يُضيف عمود "الماركة_المعتمدة":
      - تطابق ≥ 75%: الاسم الحرفي من ملف سلة.
      - لا تطابق: الاسم الأصلي (لا قيمة فارغة أبداً إذا كان هناك ماركة).

    يُرجع: (DataFrame_المُحدَّث, قائمة_الماركات_غير_المسجلة)
    """
    if missing_df is None or missing_df.empty:
        return missing_df, []

    from utils.data_paths import get_catalog_data_path
    if not brands_csv_path:
        for fname in ("ماركات مهووس.csv", "brands.csv"):
            p = get_catalog_data_path(fname)
            if os.path.exists(p):
                brands_csv_path = p
                break

    valid_brands: list = (
        _load_brands_list(brands_csv_path) if brands_csv_path else []
    )

    missing_brands: set = set()

    def _get_valid_brand(brand_name) -> str:
        bname = str(brand_name or "").strip()
        if not bname or bname.lower() in ("nan", "none", ""):
            return "غير محدد"          # ← Hard Rule: لا قيمة فارغة أبداً
        if not valid_brands:
            return bname
        # مطابقة مباشرة (حساسة للحالة)
        for v in valid_brands:
            if bname.lower() == v.lower():
                return v
            # مقارنة الجزء الإنجليزي بعد |
            parts = [p.strip() for p in v.split("|")]
            if any(bname.lower() == p.lower() for p in parts):
                return v
        # fuzzy
        hit = rf_proc.extractOne(bname, valid_brands, scorer=rf_fuzz.token_set_ratio)
        if hit and hit[1] >= 75:
            return hit[0]
        # ماركة غير مسجلة — أبقِ الاسم الأصلي ولا تُرجع فارغاً
        missing_brands.add(bname)
        return bname

    out = missing_df.copy()
    brand_src = "الماركة" if "الماركة" in out.columns else None
    out["الماركة_المعتمدة"] = (
        out[brand_src].apply(_get_valid_brand) if brand_src else ""
    )
    return out, sorted(missing_brands)


# ══════════════════════════════════════════════════════════════════════════════
#  ذاكرة المنافسين التراكمية — Competitor Master Catalog (Upsert Logic)
# ══════════════════════════════════════════════════════════════════════════════

def upsert_competitors(new_comp_dfs: dict) -> tuple:  # (dict, int, int)
    """
    يدمج الملفات الجديدة المرفوعة مع الكتالوج المركزي المحفوظ على القرص.

    القواعد:
    - إذا كان المنتج موجوداً (نفس المنافس + نفس الرابط): يُحدَّث السعر وتاريخ الرصد.
    - إذا كان المنتج جديداً: يُضاف.
    - لا يُحذف أي منتج قديم إطلاقاً.

    يُرجع: master_df كـ dict {اسم_المتجر: DataFrame} جاهز للتمرير لـ run_full_analysis.
    """
    from utils.data_paths import get_master_competitors_path

    master_path = get_master_competitors_path()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── دمج الملفات الجديدة إن وُجدت ──────────────────────────────────────
    if new_comp_dfs:
        frames = []
        for store_name, df in new_comp_dfs.items():
            df = df.copy()
            # حقن اسم المتجر إذا لم يكن موجوداً
            if "store" not in df.columns and "المنافس" not in df.columns:
                df["store"] = store_name
            df["تاريخ_الرصد"] = now_str
            frames.append(df)
        new_df = pd.concat(frames, ignore_index=True)
    else:
        new_df = pd.DataFrame()

    # ── تحميل الكتالوج القديم ──────────────────────────────────────────────
    if os.path.exists(master_path):
        try:
            master_df = pd.read_csv(master_path, encoding="utf-8-sig", low_memory=False)
        except Exception:
            master_df = pd.DataFrame()
    else:
        master_df = pd.DataFrame()

    # ── دمج القديم + الجديد ────────────────────────────────────────────────
    if new_df.empty and master_df.empty:
        return {}

    if new_df.empty:
        combined = master_df
    elif master_df.empty:
        combined = new_df
    else:
        combined = pd.concat([master_df, new_df], ignore_index=True)

    # ── تطبيع عمود الرصد حتى يُمكن الفرز عليه ───────────────────────────
    if "تاريخ_الرصد" not in combined.columns:
        combined["تاريخ_الرصد"] = now_str

    combined["تاريخ_الرصد"] = combined["تاريخ_الرصد"].fillna(now_str).astype(str)
    combined = combined.sort_values("تاريخ_الرصد", kind="stable")

    # ── إزالة التكرار: الأحدث يربح (keep='last' بعد الفرز) ───────────────
    store_col = next((c for c in ["المنافس", "store", "متجر"] if c in combined.columns), None)
    url_col = next((c for c in ["رابط_المنافس", "رابط المنتج", "url", "link", "رابط"] if c in combined.columns), None)

    if store_col and url_col:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[store_col, url_col], keep="last")
        deduped = before - len(combined)
    else:
        deduped = 0

    combined = combined.reset_index(drop=True)

    # ── حفظ الكتالوج المتراكم ─────────────────────────────────────────────
    try:
        combined.to_csv(master_path, index=False, encoding="utf-8-sig")
    except Exception as _e:
        import logging as _logging
        _logging.getLogger(__name__).error(
            "upsert_competitors: فشل حفظ الكتالوج المتراكم في '%s' — %s", master_path, _e
        )

    # ── إرجاع dict مقسَّم حسب المتجر (لـ run_full_analysis) ──────────────
    result: dict = {}
    split_col = store_col  # قد يكون "store" أو "المنافس" أو None
    if split_col and split_col in combined.columns:
        for sname, sdf in combined.groupby(split_col, sort=False):
            result[str(sname).strip() or "master_competitors"] = sdf.reset_index(drop=True)
    else:
        result["master_competitors.csv"] = combined

    return result, len(combined), deduped


# ══════════════════════════════════════════════════════════════════════════════
#  filter_unique_competitors — مُحرّك تصفية تكرار المنافسين (Presentation Layer)
#  يُطبَّق على مستوى العرض فقط — لا يمسّ قاعدة البيانات
# ══════════════════════════════════════════════════════════════════════════════

_DOMAIN_FROM_URL_RE = re.compile(
    r"https?://(?:www\.)?([a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,})",
    re.I,
)


def _comp_dedup_key(comp: dict) -> str:
    """
    يُنتج مفتاح تجميع فريداً لكل إدخال منافس.
    الأولوية: دومين من product_url → دومين من competitor → competitor نصاً.
    """
    for url_field in ("product_url", "url", "comp_url"):
        raw_url = str(comp.get(url_field) or "").strip()
        if raw_url.startswith("http"):
            m = _DOMAIN_FROM_URL_RE.search(raw_url)
            if m:
                return m.group(1).lower().lstrip("www.")
    # اسم المتجر/المنافس مباشرةً (مع تطبيع)
    c = str(comp.get("competitor") or "").strip().lower()
    c = c.replace("www.", "")
    # إزالة امتداد .csv إن كان ملف منافس
    c = re.sub(r"\.csv$", "", c).strip()
    return c or "unknown"


def filter_unique_competitors(all_comps) -> list:
    """
    يُزيل تكرار نفس المنافس (domain/store) في قائمة المطابقات — Presentation Layer.

    القاعدة: نفس المنافس → تُحفظ المطابقة ذات أعلى score فقط.
    في حال تساوي الـ score → تُفضَّل الأقل سعراً.

    يدعم أيضاً:
    - قوائم فارغة / None
    - JSON string (عند تحميل DataFrame من CSV)

    لا يُعدِّل قاعدة البيانات — للعرض فقط.
    """
    if not all_comps:
        return []

    # ── دعم السلسلة النصية (تحميل من CSV) ─────────────────────────────────
    if isinstance(all_comps, str):
        import json as _json
        try:
            all_comps = _json.loads(all_comps)
        except (ValueError, TypeError):
            return []

    if not isinstance(all_comps, list):
        return []

    # ── بناء خريطة: dedup_key → أفضل مرشح ──────────────────────────────────
    best: dict[str, dict] = {}
    for comp in all_comps:
        if not isinstance(comp, dict):
            continue
        key  = _comp_dedup_key(comp)
        score = float(comp.get("score") or 0)
        price = float(comp.get("price") or 0)

        if key not in best:
            best[key] = comp
        else:
            prev       = best[key]
            prev_score = float(prev.get("score") or 0)
            prev_price = float(prev.get("price") or 0)
            # أعلى score ينتصر؛ عند التساوي → الأقل سعراً
            if score > prev_score or (score == prev_score and price < prev_price):
                best[key] = comp

    return list(best.values())
