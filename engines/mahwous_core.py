"""
mahwous_core — فلاتر مسار صارمة ومدقق تصدير متوافق مع سلة / Make.
لا يستورد من engines.engine لتجنب الاستيراد الدائري.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# جدول ترجمة الأرقام العربية-الهندية → ASCII
_AR_DIGIT_TABLE = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

import pandas as pd

try:
    from config import REJECT_KEYWORDS
except ImportError:
    REJECT_KEYWORDS = [
        "sample", "عينة", "عينه", "decant", "تقسيم", "تقسيمة",
        "split", "miniature", "0.5ml", "1ml", "2ml", "3ml",
    ]


def _safe_float(val: Any, default: float = 0.0) -> float:
    """
    تحويل آمن إلى float — يدعم الأرقام العربية-الهندية ورموز العملة.
    مثال: "350 ر.س" → 350.0 | "1,500 SAR" → 1500.0 | "١٥٠٠" → 1500.0
    """
    try:
        if val is None or (isinstance(val, float) and val != val):
            return default
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if s in ("", "nan", "None", "NaN"):
            return default
        # ترجمة الأرقام العربية-الهندية
        s = s.translate(_AR_DIGIT_TABLE)
        # إزالة رموز العملة والنصوص، الإبقاء على الأرقام والفاصلتين والناقص فقط
        s = re.sub(r'[^\d.,-]', '', s)
        # إزالة الفاصلة كفاصل آلاف
        s = s.replace(',', '')
        return float(s) if s else default
    except (ValueError, TypeError):
        return default


def _is_sample_strict(name: str) -> bool:
    if not isinstance(name, str) or not name.strip():
        return True
    nl = name.lower()
    return any(k.lower() in nl for k in REJECT_KEYWORDS)


def _extract_ml(name: str) -> float:
    """استخراج حجم بالمل من الاسم؛ ‎-1 إن لم يُعثر."""
    if not isinstance(name, str):
        return -1.0
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:ml|مل|ملي)\b", name, re.I)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return -1.0
    return -1.0


def _classify_rejected(name: str) -> bool:
    """عينات ومسارات مرفوضة للمسار الصارم (مثل classify_product rejected)."""
    if not isinstance(name, str):
        return True
    nl = name.lower()
    if any(
        w in nl
        for w in (
            "sample",
            "عينة",
            "عينه",
            "miniature",
            "مينياتشر",
            "travel size",
            "decant",
            "تقسيم",
            "split",
        )
    ):
        return True
    return False


def apply_strict_pipeline_filters(
    df: pd.DataFrame, name_col: str = "منتج_المنافس"
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    فلترة صارمة: عينات، أحجام صغيرة جداً في الاسم، وكلمات/تصنيف مرفوض.
    يعيد (dataframe_المصفّى، إحصاءات).
    """
    if df is None or df.empty:
        return df, {"dropped": 0}

    if name_col not in df.columns:
        return df.copy(), {"dropped": 0, "warning": f"عمود غير موجود: {name_col}"}

    stats: Dict[str, Any] = {
        "dropped_sample_kw": 0,
        "dropped_small_ml": 0,
        "dropped_class_rejected": 0,
        "dropped_empty_name": 0,
    }
    keep_idx: List[Any] = []

    for idx, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name or name.lower() in ("nan", "none", "<na>"):
            stats["dropped_empty_name"] += 1
            continue
        if _is_sample_strict(name):
            stats["dropped_sample_kw"] += 1
            continue
        if _classify_rejected(name):
            stats["dropped_class_rejected"] += 1
            continue
        ml = _extract_ml(name)
        if 0 < ml < 5:
            stats["dropped_small_ml"] += 1
            continue

        keep_idx.append(idx)

    out = df.loc[keep_idx].reset_index(drop=True) if keep_idx else pd.DataFrame()
    stats["dropped"] = len(df) - len(out)
    stats["kept"] = len(out)
    return out, stats


def validate_export_product_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    تحقق صارم قبل إرسال Make / تصدير: اسم صالح وسعر المنافس > 0 لكل صف.
    يتوافق مع منطق send_missing_products / export_to_make_format لقسم المفقودات.
    """
    issues: List[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتحقق أو التصدير."]

    for i, (_, row) in enumerate(df.iterrows()):
        name = (
            str(row.get("منتج_المنافس", "")).strip()
            or str(row.get("المنتج", "")).strip()
            or str(row.get("أسم المنتج", "")).strip()
            or str(row.get("اسم المنتج", "")).strip()
        )
        price = _safe_float(
            row.get("سعر_المنافس", row.get("سعر المنافس", row.get("السعر", 0)))
        )
        label = name[:48] + ("…" if len(name) > 48 else "") if name else "(بدون اسم)"

        if not name or name.lower() in ("nan", "none"):
            issues.append(f"صف {i + 1}: اسم المنتج فارغ — {label}")
        if price <= 0:
            issues.append(f"صف {i + 1}: السعر غير صالح أو صفر ({price}) — {label}")

    return (len(issues) == 0, issues)
