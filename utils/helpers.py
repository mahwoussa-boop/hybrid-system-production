"""
utils/helpers.py - دوال مساعدة v17.2
الملف الذي كان مفقوداً - يحتوي على جميع الدوال المستوردة في app.py
"""
import html as html_std
import io
import re
from typing import Optional, Dict, List
from urllib.parse import urlparse

import pandas as pd
import requests


# ===== safe_float =====
_AR_DIGIT_TABLE = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def safe_float(val, default=0.0) -> float:
    """تحويل قيمة إلى float بأمان — يدعم الأرقام العربية والفواصل ورموز العملة."""
    try:
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
            return default
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        # ترجمة الأرقام العربية-الهندية
        s = s.translate(_AR_DIGIT_TABLE)
        # إزالة رموز العملة والمسافات، الإبقاء على الأرقام والفاصلتين فقط
        s = re.sub(r'[^\d.,-]', '', s)
        # إزالة الفاصلة كفاصل آلاف (السياق: أسعار ريال سعودي)
        s = s.replace(',', '')
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


# ===== format_price =====
def format_price(price, currency="ر.س") -> str:
    """تنسيق عرض السعر"""
    try:
        return f"{float(price):,.0f} {currency}"
    except:
        return f"0 {currency}"


# ===== format_diff =====
def format_diff(diff) -> str:
    """تنسيق عرض فرق السعر"""
    try:
        d = float(diff)
        sign = "+" if d > 0 else ""
        return f"{sign}{d:,.0f} ر.س"
    except:
        return "0 ر.س"


# ===== get_filter_options =====
def get_filter_options(df: pd.DataFrame) -> dict:
    """استخراج خيارات الفلاتر من DataFrame"""
    opts = {
        "brands": ["الكل"],
        "competitors": ["الكل"],
        "types": ["الكل"],
    }
    if df is None or df.empty:
        return opts

    # الماركات
    if "الماركة" in df.columns:
        brands = df["الماركة"].dropna().unique().tolist()
        brands = sorted([str(b) for b in brands if str(b).strip() and str(b) != "nan"])
        opts["brands"] = ["الكل"] + brands

    # المنافسون
    if "المنافس" in df.columns:
        comps = df["المنافس"].dropna().unique().tolist()
        comps = sorted([str(c) for c in comps if str(c).strip() and str(c) != "nan"])
        opts["competitors"] = ["الكل"] + comps

    # الأنواع
    if "النوع" in df.columns:
        types = df["النوع"].dropna().unique().tolist()
        types = sorted([str(t) for t in types if str(t).strip() and str(t) != "nan"])
        opts["types"] = ["الكل"] + types

    return opts


# ===== apply_filters =====
def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """تطبيق الفلاتر على DataFrame"""
    if df is None or df.empty:
        return df

    result = df.copy()

    # بحث نصي
    search = filters.get("search", "").strip()
    if search:
        mask = pd.Series([False] * len(result))
        for col in ["المنتج", "منتج_المنافس", "الماركة"]:
            if col in result.columns:
                mask = mask | result[col].astype(str).str.contains(search, case=False, na=False)
        result = result[mask]

    # فلتر الماركة
    brand = filters.get("brand", "الكل")
    if brand and brand != "الكل" and "الماركة" in result.columns:
        result = result[result["الماركة"].astype(str) == brand]

    # فلتر المنافس
    competitor = filters.get("competitor", "الكل")
    if competitor and competitor != "الكل" and "المنافس" in result.columns:
        result = result[result["المنافس"].astype(str) == competitor]

    # فلتر النوع
    ptype = filters.get("type", "الكل")
    if ptype and ptype != "الكل" and "النوع" in result.columns:
        result = result[result["النوع"].astype(str) == ptype]

    # فلتر نسبة التطابق
    match_min = filters.get("match_min")
    if match_min and "نسبة_التطابق" in result.columns:
        result = result[result["نسبة_التطابق"] >= float(match_min)]

    # فلتر أقل سعر
    price_min = filters.get("price_min", 0.0)
    if price_min and price_min > 0 and "السعر" in result.columns:
        result = result[result["السعر"] >= float(price_min)]

    # فلتر أعلى سعر
    price_max = filters.get("price_max")
    if price_max and price_max > 0 and "السعر" in result.columns:
        result = result[result["السعر"] <= float(price_max)]

    return result.reset_index(drop=True)


# ===== export_to_excel =====
def export_to_excel(df: pd.DataFrame, sheet_name: str = "النتائج") -> bytes:
    """تصدير DataFrame إلى Excel"""
    output = io.BytesIO()
    export_df = df.copy()

    # إزالة الأعمدة غير القابلة للتسلسل
    for col in ["جميع المنافسين", "جميع_المنافسين"]:
        if col in export_df.columns:
            export_df = export_df.drop(columns=[col])

    safe_name = sheet_name[:31]
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, sheet_name=safe_name, index=False)

        # تنسيق العمود
        ws = writer.sheets[safe_name]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== export_multiple_sheets =====
def export_multiple_sheets(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """تصدير عدة DataFrames في ملف Excel متعدد الأوراق"""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            export_df = df.copy()
            for col in ["جميع المنافسين", "جميع_المنافسين"]:
                if col in export_df.columns:
                    export_df = export_df.drop(columns=[col])

            safe_name = str(sheet_name)[:31]
            export_df.to_excel(writer, sheet_name=safe_name, index=False)

            # تنسيق تلقائي
            ws = writer.sheets[safe_name]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== parse_pasted_text =====
def parse_pasted_text(text: str):
    """
    تحليل نص ملصوق وتحويله إلى DataFrame
    يدعم: CSV، TSV، جداول مفصولة بـ |
    """
    if not text or not text.strip():
        return None, "النص فارغ"

    lines = [l.strip() for l in text.strip().split('\n') if l.strip()]

    if not lines:
        return None, "لا توجد بيانات"

    # محاولة 1: مفصول بـ |
    if '|' in lines[0]:
        rows = []
        for line in lines:
            if set(line.replace(' ', '').replace('-', '')) == {'|'}:
                continue  # تخطي خطوط الفاصل
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if cells:
                rows.append(cells)

        if len(rows) >= 2:
            try:
                df = pd.DataFrame(rows[1:], columns=rows[0])
                return df, f"✅ تم تحليل {len(df)} صف"
            except:
                pass

    # محاولة 2: TSV (tabs)
    if '\t' in lines[0]:
        try:
            df = pd.read_csv(io.StringIO(text), sep='\t')
            return df, f"✅ تم تحليل {len(df)} صف (TSV)"
        except:
            pass

    # محاولة 3: CSV
    try:
        df = pd.read_csv(io.StringIO(text))
        return df, f"✅ تم تحليل {len(df)} صف (CSV)"
    except:
        pass

    # محاولة 4: كل سطر منتج
    if len(lines) >= 2:
        df = pd.DataFrame({"البيانات": lines})
        return df, f"✅ تم تحليل {len(df)} سطر"

    return None, "❌ لا يمكن تحليل الصيغة. جرب CSV أو جدول مفصول بـ |"


# ===== صورة من صفحة المنتج (عند غياب عمود صورة في ملف المنافس) =====
def fetch_og_image_url(url: str, timeout: float = 6.0) -> str:
    """يجلب og:image (أو twitter:image) من HTML صفحة المنتج."""
    u = (url or "").strip()
    if not u.startswith("http"):
        return ""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        }
        r = requests.get(u, timeout=timeout, headers=headers, allow_redirects=True)
        if r.status_code != 200:
            return ""
        text = r.text[:900_000]
        patterns = (
            re.compile(
                r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
                re.I,
            ),
            re.compile(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                re.I,
            ),
            re.compile(
                r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
                re.I,
            ),
        )
        for pat in patterns:
            m = pat.search(text)
            if m:
                img = (m.group(1) or "").strip()
                if img.startswith("https://") or img.startswith("http://"):
                    return img
                if img.startswith("//"):
                    return "https:" + img
    except Exception:
        pass
    return ""


def fetch_page_title_from_url(url: str, timeout: float = 8.0) -> str:
    """
    يجلب عنواناً مقروءاً من صفحة المنتج: og:title ثم twitter:title ثم <title>.
    يُنظّف لاحقة المتجر الشائعة ( | Site — متجر).
    """
    u = (url or "").strip()
    if not u.startswith("http"):
        return ""
    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
        }
        r = requests.get(u, timeout=timeout, headers=headers, allow_redirects=True)
        if r.status_code != 200:
            return ""
        text = r.text[:900_000]
        raw = ""
        for pat in (
            re.compile(
                r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
                re.I,
            ),
            re.compile(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
                re.I,
            ),
            re.compile(
                r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
                re.I,
            ),
            re.compile(r"<title[^>]*>([^<]{4,500})</title>", re.I | re.DOTALL),
        ):
            m = pat.search(text)
            if m:
                raw = (m.group(1) or "").strip()
                if raw:
                    break
        if not raw:
            return ""
        title = html_std.unescape(raw).strip()
        # أسطر متعددة (og:title أحياناً يكرر السطر مع/بدون حجم): نظّف كل سطر ثم خذ الأوضح
        _lines = []
        for ln in re.split(r"[\r\n]+", title):
            ln = ln.strip()
            if not ln:
                continue
            # بادئة «محلي» الشائعة في متاجر سعودية
            ln = re.sub(r"^محلي\s*[-–—:،]\s*", "", ln)
            ln = re.sub(r"^محلي\s+", "", ln).strip()
            ln = re.sub(r"\s+", " ", ln)
            if ln:
                _lines.append(ln)
        if _lines:
            title = max(_lines, key=len)
        else:
            title = re.sub(r"\s+", " ", title).strip()
        # إزالة لاحقة اسم المتجر الشائعة
        for sep in (" | ", " – ", " — ", " - ", " :: "):
            if sep in title:
                left = title.split(sep)[0].strip()
                if len(left) >= 6:
                    title = left
                    break
        # إزالة بادئات عامة
        title = re.sub(r"^(buy|shop|تسوق|اشتري)\s+", "", title, flags=re.I).strip()
        title = re.sub(r"^محلي\s*[-–—:،]\s*", "", title).strip()
        title = re.sub(r"^محلي\s+", "", title).strip()
        return title[:220] if title else ""
    except Exception:
        return ""


def favicon_url_for_site(page_url: str) -> str:
    """أيقونة موجّهة من خدمة عامة — احتياط عند فشل og:image."""
    u = (page_url or "").strip()
    if not u.startswith("http"):
        return ""
    try:
        netloc = urlparse(u).netloc
        if not netloc:
            return ""
        return f"https://www.google.com/s2/favicons?domain={netloc}&sz=128"
    except Exception:
        return ""


# ===== BackgroundTask (stub) =====
class BackgroundTask:
    """
    محاكاة معالجة في الخلفية
    ملاحظة: Streamlit لا يدعم true background threads بشكل كامل
    هذا placeholder وظيفي
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.done = False
        self.error = None

    def run(self):
        """تشغيل المهمة مباشرة (synchronous)"""
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.done = True
        except Exception as e:
            self.error = str(e)
            self.done = True
        return self.result

    def is_done(self):
        return self.done
