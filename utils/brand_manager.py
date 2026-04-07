"""
utils/brand_manager.py v1.0 — نظام إدارة الماركات العالمية المفقودة
════════════════════════════════════════════════════════════════════════
✅ كشف الماركات المفقودة بـ Fuzzy Matching دقيق (RapidFuzz ≥ 75%)
✅ منع التكرار الصارم — normalize_key يوحّد الأسماء قبل المقارنة
✅ توليد بيانات سلة الكاملة عبر Gemini (اسم، وصف، SEO)
✅ تصدير Brands_Salla.csv بأعمدة سلة الرسمية (7 أعمدة)
✅ توليد Visual Prompt لجلب لوجو الماركة بالـ AI
✅ Thread-safe — BrandManager singleton آمن للاستخدام المتزامن
✅ ربط المنتج بالماركة عبر canonical_name موحّد

الأعمدة الرسمية لـ Brands_Salla.csv (من قالب سلة المرفوع):
 1. اسم الماركة                             ← max 30 حرف
 2. وصف مختصر عن الماركة                   ← max 255 حرف
 3. صورة شعار الماركة                       ← رابط URL مباشر (فارغ إن لم يتوفر)
 4. (إختياري) صورة البانر                   ← فارغ دائماً
 5. (Page Title) عنوان صفحة العلامة التجارية ← max 70 حرف
 6. (SEO Page URL) رابط صفحة العلامة التجارية
 7. (Page Description) وصف صفحة العلامة التجارية ← max 155 حرف
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import threading
import unicodedata
from typing import Optional

_logger = logging.getLogger(__name__)

# ── حدود أحرف سلة (Salla Character Limits) ──────────────────────────────
_MAX_BRAND_NAME = 30
_MAX_BRAND_DESC = 255
_MAX_PAGE_TITLE = 70
_MAX_META_DESC  = 155

# ── أعمدة ملف Brands_Salla.csv (بالترتيب والاسم الحرفي من قالب سلة) ─────
SALLA_BRAND_COLUMNS = [
    "اسم الماركة",
    "وصف مختصر عن الماركة",
    "صورة شعار الماركة",
    "(إختياري) صورة البانر",
    "(Page Title) عنوان صفحة العلامة التجارية",
    "(SEO Page URL) رابط صفحة العلامة التجارية",
    "(Page Description) وصف صفحة العلامة التجارية",
]

# ── مسار ملف الجلسة (session cache) ──────────────────────────────────────
def _session_cache_path() -> str:
    from utils.data_paths import get_catalog_data_path
    return get_catalog_data_path("new_brands_session.json")


# ══════════════════════════════════════════════════════════════════════════
#  دوال تطبيع الأسماء للمقارنة (Normalization for Deduplication)
# ══════════════════════════════════════════════════════════════════════════

def normalize_key(name: str) -> str:
    """
    يُنتج مفتاح مقارنة موحّد من اسم الماركة:
    1. إزالة التشكيل والحركات العربية.
    2. تحويل لأحرف صغيرة (lowercase).
    3. إزالة المسافات والشرطات والرموز غير الحرفية.
    4. توحيد همزات الألف (أ إ آ → ا).

    مثال: "ديور | Dior" → "diorديور"
    مثال: "Christian  Dior" → "christiandior"
    """
    if not name:
        return ""
    s = str(name).strip()

    # إزالة التشكيل العربي (Unicode category Mn = Non-spacing marks)
    s = "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )

    # توحيد همزات الألف
    for variant in ("أ", "إ", "آ", "ٱ"):
        s = s.replace(variant, "ا")

    # lowercase
    s = s.lower()

    # احتفظ بالحروف والأرقام العربية والإنجليزية فقط
    s = re.sub(r"[^\w\u0600-\u06FF]", "", s, flags=re.UNICODE)
    return s.strip()


def _names_are_same(a: str, b: str) -> bool:
    """مقارنة سريعة بالـ normalize_key — بدون fuzzy."""
    return bool(normalize_key(a) == normalize_key(b))


# ══════════════════════════════════════════════════════════════════════════
#  BrandManager — Singleton thread-safe
# ══════════════════════════════════════════════════════════════════════════

class BrandManager:
    """
    مدير الماركات — يتتبع الماركات الموجودة والمكتشفة حديثاً.

    الاستخدام:
        mgr = BrandManager.get_instance()
        canonical, is_new = mgr.resolve(raw_brand)
        csv_bytes = mgr.export_brands_csv()
    """

    _instance: Optional["BrandManager"] = None
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def get_instance(cls) -> "BrandManager":
        """Singleton — يُنشئ مرة واحدة فقط لكل عملية."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """يعيد ضبط الـ Singleton — مفيد للاختبارات أو بعد رفع ملفات جديدة."""
        with cls._lock:
            cls._instance = None

    def __init__(self) -> None:
        self._rw_lock = threading.RLock()          # قفل للقراءة/الكتابة
        self._known: dict[str, str] = {}           # key → canonical_name (من CSV)
        self._new: dict[str, dict] = {}            # key → brand_data (مكتشفة جديدة)
        self._new_canonical: dict[str, str] = {}   # key → canonical_name للجديدة
        self._loaded = False
        self._load_known_brands()
        self._restore_session()

    # ── تحميل الماركات المعتمدة من CSV ───────────────────────────────────

    def _load_known_brands(self) -> None:
        """يقرأ brands.csv (أو ماركات مهووس.csv) ويبني فهرس normalize_key."""
        from utils.data_paths import get_catalog_data_path
        import pandas as _pd

        search_order = [
            ("ماركات مهووس.csv", True),
            ("brands.csv",        False),
        ]
        for fname, is_salla in search_order:
            fpath = get_catalog_data_path(fname)
            if not os.path.exists(fpath):
                continue
            for enc in ("utf-8-sig", "utf-8", "cp1256"):
                try:
                    df = _pd.read_csv(fpath, encoding=enc)
                    col = df.columns[0]
                    with self._rw_lock:
                        for v in df[col].dropna().tolist():
                            canonical = str(v).strip()
                            if canonical and canonical.lower() not in ("nan", "none"):
                                self._known[normalize_key(canonical)] = canonical
                    _logger.info(
                        "BrandManager: تحميل %d ماركة من %s",
                        len(self._known), fpath,
                    )
                    self._loaded = True
                    return
                except Exception:
                    continue
        _logger.warning("BrandManager: لم يُعثر على ملف ماركات صالح")

    # ── استعادة الجلسة السابقة ────────────────────────────────────────────

    def _restore_session(self) -> None:
        """يُعيد تحميل الماركات الجديدة من الجلسة السابقة (إن وُجدت)."""
        path = _session_cache_path()
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            with self._rw_lock:
                for key, brand_data in data.items():
                    if key not in self._new:
                        self._new[key] = brand_data
                        canonical = brand_data.get("brand_name", "")
                        if canonical:
                            self._new_canonical[key] = canonical
            _logger.info(
                "BrandManager: استُعيدت %d ماركة جديدة من الجلسة",
                len(self._new),
            )
        except Exception as e:
            _logger.warning("BrandManager: فشل استعادة الجلسة — %s", e)

    def _save_session(self) -> None:
        """يحفظ الماركات الجديدة في ملف JSON للاستعادة لاحقاً."""
        path = _session_cache_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(self._new, fh, ensure_ascii=False, indent=2)
        except Exception as e:
            _logger.warning("BrandManager: فشل حفظ الجلسة — %s", e)

    # ── الدالة الرئيسية: resolve ──────────────────────────────────────────

    def resolve(
        self,
        raw_brand: str,
        auto_generate: bool = True,
    ) -> tuple[str, bool]:
        """
        يُحدد الاسم الرسمي للماركة ويُسجّل الجديدة.

        المنطق:
        1. إذا كانت الماركة موجودة في _new → أعد canonical_name المسجّل مسبقاً.
        2. إذا كانت الماركة موجودة في _known (fuzzy ≥ 75%) → أعد الاسم المعتمد.
        3. إذا لم تُوجد:
           a. إذا auto_generate=True → استدعِ AI لتوليد بيانات الماركة.
           b. سجّلها في _new.
           c. أعد (canonical_name, True).

        Returns:
            (canonical_name: str, is_new: bool)
        """
        raw = str(raw_brand or "").strip()
        if not raw or raw.lower() in ("nan", "none", "غير محدد", ""):
            return "غير محدد", False

        key = normalize_key(raw)

        # ── 1. فحص الماركات الجديدة أولاً (من نفس الجلسة) ──────────────
        with self._rw_lock:
            if key in self._new_canonical:
                return self._new_canonical[key], False  # مُسجَّلة سابقاً كجديدة

        # ── 2. fuzzy matching مع الماركات المعتمدة ──────────────────────
        matched = self._fuzzy_match_known(raw)
        if matched:
            return matched, False

        # ── 3. ماركة جديدة غير موجودة ───────────────────────────────────
        _logger.info("BrandManager: ماركة جديدة مكتشفة — «%s»", raw)

        brand_data: dict = {}
        if auto_generate:
            brand_data = self._generate_brand_data(raw)
        else:
            brand_data = _minimal_brand_data(raw)

        canonical = brand_data.get("brand_name") or raw

        with self._rw_lock:
            # تحقق مرة أخرى من التكرار (thread safety)
            if key in self._new_canonical:
                return self._new_canonical[key], False
            self._new[key] = brand_data
            self._new_canonical[key] = canonical

        self._save_session()
        return canonical, True

    # ── Fuzzy Matching ─────────────────────────────────────────────────────

    def _fuzzy_match_known(self, raw: str) -> Optional[str]:
        """
        يبحث في الماركات المعتمدة بمراحل:
        1. مطابقة مباشرة بـ normalize_key (أسرع).
        2. مطابقة الجزء الإنجليزي أو العربي منفصلاً (للأسماء الثنائية مثل ديور | Dior).
        3. rapidfuzz token_set_ratio على الأسماء الأصلية (≥ 75%).
        """
        with self._rw_lock:
            known_copy = dict(self._known)  # key → canonical

        if not known_copy:
            return None

        raw_key   = normalize_key(raw)
        raw_lower = raw.strip().lower()

        # ── مرحلة 1: مطابقة مباشرة بالـ key ─────────────────────────────
        if raw_key in known_copy:
            return known_copy[raw_key]

        # ── مرحلة 2: بحث جزئي في أجزاء الاسم الثنائي ────────────────────
        # بناء فهرس مقلوب: جزء_مطبّع → canonical
        parts_index: dict[str, str] = {}
        for canonical in known_copy.values():
            parts = [p.strip() for p in canonical.replace("|", " | ").split("|")]
            for part in parts:
                pkey = normalize_key(part)
                if pkey and len(pkey) >= 3:
                    parts_index[pkey] = canonical

        if raw_key in parts_index:
            return parts_index[raw_key]

        # ── مرحلة 3: rapidfuzz على الأسماء الأصلية ───────────────────────
        try:
            from rapidfuzz import process as rf_proc, fuzz as rf_fuzz
            all_canonicals = list(known_copy.values())
            all_lower      = [c.lower() for c in all_canonicals]

            # a) مطابقة على الاسم الأصلي الكامل
            hit = rf_proc.extractOne(
                raw_lower,
                all_lower,
                scorer=rf_fuzz.token_set_ratio,
            )
            if hit and hit[1] >= 75:
                idx = all_lower.index(hit[0])
                return all_canonicals[idx]

            # b) مطابقة على الجزء الإنجليزي فقط من كل ماركة ثنائية
            en_parts: list[tuple[str, str]] = []  # (en_lower, canonical)
            for c in all_canonicals:
                for seg in c.split("|"):
                    seg = seg.strip()
                    if re.search(r"[a-zA-Z]", seg) and len(seg) >= 2:
                        en_parts.append((seg.lower(), c))
            if en_parts:
                en_hit = rf_proc.extractOne(
                    raw_lower,
                    [ep[0] for ep in en_parts],
                    scorer=rf_fuzz.token_set_ratio,
                )
                if en_hit and en_hit[1] >= 75:
                    matched_en = en_hit[0]
                    canonical_match = next(
                        (c for en, c in en_parts if en == matched_en), None
                    )
                    if canonical_match:
                        return canonical_match

        except ImportError:
            # fallback بسيط: containment check
            for canonical in known_copy.values():
                if raw_lower in canonical.lower() or canonical.lower() in raw_lower:
                    return canonical
                for part in canonical.split("|"):
                    if raw_lower == part.strip().lower():
                        return canonical
        return None

    # ── توليد بيانات الماركة عبر AI ──────────────────────────────────────

    def _generate_brand_data(self, brand_name: str) -> dict:
        """يستدعي generate_salla_brand_info من ai_engine ويضمن الحدود."""
        try:
            from engines.ai_engine import generate_salla_brand_info
            data = generate_salla_brand_info(brand_name)
            if data and data.get("brand_name"):
                return data
        except Exception as e:
            _logger.warning("BrandManager._generate_brand_data: %s", e)
        return _minimal_brand_data(brand_name)

    # ── حالة الماركات ─────────────────────────────────────────────────────

    def get_new_brands(self) -> list[dict]:
        """يُعيد قائمة الماركات الجديدة المكتشفة في هذه الجلسة."""
        with self._rw_lock:
            return list(self._new.values())

    def get_new_count(self) -> int:
        with self._rw_lock:
            return len(self._new)

    def clear_new_brands(self) -> None:
        """يمسح سجل الماركات الجديدة (بعد التصدير الناجح)."""
        with self._rw_lock:
            self._new.clear()
            self._new_canonical.clear()
        path = _session_cache_path()
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    def reload_known_brands(self) -> None:
        """يُعيد تحميل ملف الماركات (بعد رفع ملف جديد من الواجهة)."""
        with self._rw_lock:
            self._known.clear()
        self._load_known_brands()

    # ── تصدير Brands_Salla.csv ────────────────────────────────────────────

    def export_brands_csv(self) -> bytes:
        """
        يُنشئ ملف Brands_Salla.csv جاهزاً لرفعه على سلة.

        الهيكل:
          الصف 1: رؤوس الأعمدة (SALLA_BRAND_COLUMNS)
          الصف 2+: بيانات الماركات الجديدة (بدون تكرار)

        الترميز: UTF-8 مع BOM (utf-8-sig)
        """
        buf    = io.StringIO(newline="")
        writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(SALLA_BRAND_COLUMNS)

        seen_keys: set[str] = set()
        with self._rw_lock:
            brands = list(self._new.values())

        for bd in brands:
            canonical = str(bd.get("brand_name", "") or "").strip()
            if not canonical:
                continue
            k = normalize_key(canonical)
            if k in seen_keys:
                continue          # منع التكرار داخل الملف نفسه
            seen_keys.add(k)

            row = {
                "اسم الماركة":                                  _clamp(canonical,                      _MAX_BRAND_NAME),
                "وصف مختصر عن الماركة":                        _clamp(bd.get("description", ""),      _MAX_BRAND_DESC),
                "صورة شعار الماركة":                            str(bd.get("logo_url",  "") or "").strip(),
                "(إختياري) صورة البانر":                        "",
                "(Page Title) عنوان صفحة العلامة التجارية":    _clamp(bd.get("seo_title", ""),        _MAX_PAGE_TITLE),
                "(SEO Page URL) رابط صفحة العلامة التجارية":   _safe_seo_url(bd.get("seo_url", ""), canonical),
                "(Page Description) وصف صفحة العلامة التجارية": _clamp(bd.get("seo_desc", ""),       _MAX_META_DESC),
            }
            writer.writerow([row[col] for col in SALLA_BRAND_COLUMNS])

        return ("\ufeff" + buf.getvalue()).encode("utf-8")

    # ── Visual Prompt ─────────────────────────────────────────────────────

    def generate_visual_prompt(self, brand_name: str) -> str:
        """
        يولّد برومت بصري احترافي لجلب لوجو الماركة عبر AI.
        الناتج: نص إنجليزي مُحكم يُستخدم مع DALL-E / Midjourney / Flux.
        """
        clean = str(brand_name or "").strip()
        # استخرج الجزء الإنجليزي إن وُجد
        if "|" in clean:
            parts  = [p.strip() for p in clean.split("|")]
            en_name = next((p for p in parts if re.search(r"[a-zA-Z]", p)), parts[-1])
        else:
            en_name = clean if re.search(r"[a-zA-Z]", clean) else clean

        return (
            f'Professional luxury brand logo for "{en_name}", '
            f"a high-end perfume and fragrance house. "
            f"Minimalist elegant design on pure white background. "
            f"Gold and black color palette. Premium typography, serif font. "
            f"Suitable for e-commerce product listing. "
            f"No text overlays, no watermarks. 1:1 square format. "
            f"Ultra high resolution, photorealistic product shot quality."
        )


# ══════════════════════════════════════════════════════════════════════════
#  دوال مساعدة (Helpers)
# ══════════════════════════════════════════════════════════════════════════

def _clamp(value: str, max_len: int) -> str:
    """يقطع النص عند max_len مع احترام الكلمات (لا يقطع في منتصف كلمة)."""
    s = str(value or "").strip()
    if len(s) <= max_len:
        return s
    trimmed = s[:max_len].rsplit(" ", 1)[0] if " " in s[:max_len] else s[:max_len]
    return trimmed.rstrip(".,،؛:") + "…" if trimmed else s[:max_len]


def _safe_seo_url(raw_url: str, brand_name: str) -> str:
    """
    يُنتج رابط SEO آمن:
    - يُفضّل raw_url إن كان صالحاً.
    - يستخرج الجزء الإنجليزي من brand_name إن لم يكن.
    - يضيف _mahwous كـ suffix إن لم يكن موجوداً.
    """
    url = str(raw_url or "").strip().lower()
    url = re.sub(r"\s+", "_", url)
    url = re.sub(r"[^a-z0-9_\u0600-\u06FF-]", "", url)

    if url and len(url) >= 3:
        if "mahwous" not in url:
            url = (url + "_mahwous")[:80]
        return url

    # استخراج الجزء الإنجليزي من اسم الماركة
    clean_brand = str(brand_name or "").strip()
    if "|" in clean_brand:
        parts   = [p.strip() for p in clean_brand.split("|")]
        en_part = next((p for p in parts if re.search(r"[a-zA-Z]", p)), parts[-1])
    else:
        en_part = clean_brand

    slug = re.sub(r"[^a-z0-9]+", "_", en_part.lower().strip())
    slug = slug.strip("_")[:24]

    if not slug or len(slug) < 2:
        import hashlib
        slug = "brand_" + hashlib.md5(clean_brand.encode()).hexdigest()[:8]

    return f"{slug}_mahwous"


def _minimal_brand_data(brand_name: str) -> dict:
    """بيانات ماركة أساسية (Fallback إذا فشل AI)."""
    clean = str(brand_name or "").strip()[:_MAX_BRAND_NAME]
    safe  = re.sub(r"[^a-z0-9]+", "_", clean.lower())[:18].strip("_") or "brand"
    return {
        "brand_name":   clean,
        "description":  f"عطور {clean} الأصلية — اكتشف التميز والفخامة في متجر مهووس للعطور."[:_MAX_BRAND_DESC],
        "logo_url":     "",
        "logo_prompt":  "",
        "seo_title":    f"عطور {clean} الأصلية | متجر مهووس"[:_MAX_PAGE_TITLE],
        "seo_url":      f"{safe}_mahwous",
        "seo_desc":     f"تسوق عطور {clean} الأصلية بأفضل الأسعار من متجر مهووس. ضمان الأصالة وتوصيل سريع."[:_MAX_META_DESC],
    }


# ══════════════════════════════════════════════════════════════════════════
#  دوال واجهة عامة (Public API)
# ══════════════════════════════════════════════════════════════════════════

def resolve_brand(raw_brand: str, auto_generate: bool = True) -> tuple[str, bool]:
    """
    الدالة العامة — تُستدعى من salla_shamel_export.py.

    Returns:
        (canonical_name, is_new)
    """
    return BrandManager.get_instance().resolve(raw_brand, auto_generate=auto_generate)


def export_new_brands_csv() -> bytes:
    """يُصدّر ملف Brands_Salla.csv للماركات الجديدة فقط."""
    return BrandManager.get_instance().export_brands_csv()


def get_new_brands_count() -> int:
    """عدد الماركات الجديدة المكتشفة في هذه الجلسة."""
    return BrandManager.get_instance().get_new_count()


def get_new_brands_list() -> list[dict]:
    """قائمة بيانات الماركات الجديدة."""
    return BrandManager.get_instance().get_new_brands()


def get_visual_prompt(brand_name: str) -> str:
    """يُعيد برومت AI لجلب لوجو الماركة."""
    return BrandManager.get_instance().generate_visual_prompt(brand_name)


def clear_session() -> None:
    """يمسح ذاكرة الجلسة (يُستدعى بعد التصدير الناجح)."""
    BrandManager.get_instance().clear_new_brands()


def reload_brands_file() -> None:
    """يُعيد تحميل ملف الماركات (بعد رفع ملف جديد من الواجهة)."""
    BrandManager.get_instance().reload_known_brands()
