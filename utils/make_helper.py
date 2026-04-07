"""
utils/make_helper.py v24.0 — إرسال صحيح لـ Make.com
══════════════════════════════════════════════════════
سيناريو تحديث الأسعار (Integration Webhooks, Salla):
  Webhook → BasicFeeder يقرأ {{2.products}} → UpdateProduct
  Payload المطلوب: {"products": [{"product_id":"...","name":"...","price":...}]}

سيناريو المنتجات الجديدة (Mahwous - إضافة منتجات جديدة لسلة):
  Webhook → BasicFeeder يقرأ {{1.data}} → CreateProduct
  Payload المطلوب: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}

⚠️ الإصلاح الحرج v24:
   تحديث الأسعار → {"products": [{product_id, name, price, ...}]}
   المنتجات الجديدة/المفقودة → {"data": [{أسم المنتج, سعر المنتج, ...}]}
"""

import requests
import json
import os
import time
from typing import List, Dict, Any, Optional


# ── Webhook URLs ───────────────────────────────────────────────────────────
# يجب ضبط هذه المتغيرات في بيئة التشغيل (Railway / .env).
# لا يوجد fallback إنتاجي هنا — أي إرسال بدون URL سيُعيد خطأ صريحاً.
WEBHOOK_UPDATE_PRICES = os.environ.get("WEBHOOK_UPDATE_PRICES", "").strip()
WEBHOOK_NEW_PRODUCTS  = os.environ.get("WEBHOOK_NEW_PRODUCTS",  "").strip()

TIMEOUT = 15  # ثانية


# ── الإرسال الأساسي ────────────────────────────────────────────────────────
def _post_to_webhook(url: str, payload: Any) -> Dict:
    """
    إرسال بيانات JSON إلى Webhook URL.
    يُعيد dict: {"success": bool, "message": str, "status_code": int}
    """
    if not url:
        return {"success": False, "message": "❌ Webhook URL غير محدد", "status_code": 0}
    try:
        headers = {"Content-Type": "application/json"}
        resp = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=TIMEOUT
        )
        if resp.status_code in (200, 201, 202, 204):
            return {
                "success": True,
                "message": f"✅ تم الإرسال بنجاح ({resp.status_code})",
                "status_code": resp.status_code,
            }
        return {
            "success": False,
            "message": f"❌ HTTP {resp.status_code}: {resp.text[:200]}",
            "status_code": resp.status_code,
        }
    except requests.exceptions.Timeout:
        return {"success": False, "message": "❌ انتهت مهلة الاتصال (Timeout)", "status_code": 0}
    except requests.exceptions.ConnectionError:
        return {"success": False, "message": "❌ فشل الاتصال بـ Make — تحقق من الإنترنت", "status_code": 0}
    except Exception as e:
        return {"success": False, "message": f"❌ خطأ غير متوقع: {str(e)}", "status_code": 0}


# ── تحويل float آمن ───────────────────────────────────────────────────────
def _safe_float(val, default: float = 0.0) -> float:
    """تحويل آمن إلى float"""
    try:
        if val is None or str(val).strip() in ("", "nan", "None", "NaN"):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ── تنظيف product_id ──────────────────────────────────────────────────────
def _clean_pid(raw) -> str:
    """
    product_id دائماً كـ str(int(float(value)))
    مثال: 100.0 → "100" | "1081786650.0" → "1081786650"
    """
    if raw is None: return ""
    s = str(raw).strip()
    if s in ("", "nan", "None", "NaN", "0", "0.0"): return ""
    try:
        return str(int(float(s)))
    except (ValueError, TypeError):
        return s


# ══════════════════════════════════════════════════════════════════════════
#  تحويل DataFrame → قائمة منتجات مع حساب السعر الصحيح لكل قسم
# ══════════════════════════════════════════════════════════════════════════
def export_to_make_format(df, section_type: str = "update") -> List[Dict]:
    """
    تحويل DataFrame إلى قائمة منتجات جاهزة لـ Make.
    section_type: raise | lower | approved | update | missing | new
    كل منتج يحتوي على: product_id, name, price, section, + حقول سياقية
    """
    if df is None or (hasattr(df, "empty") and df.empty):
        return []

    products = []
    for _, row in df.iterrows():

        # ── رقم المنتج ────────────────────────────────────────────────────
        product_id = _clean_pid(
            row.get("معرف_المنتج")  or row.get("product_id")     or
            row.get("رقم المنتج")   or row.get("رقم_المنتج")    or
            row.get("معرف المنتج")  or row.get("sku")            or
            row.get("SKU")          or ""
        )

        # ── اسم المنتج ────────────────────────────────────────────────────
        name = (
            str(row.get("المنتج",         "")) or
            str(row.get("منتج_المنافس",   "")) or
            str(row.get("أسم المنتج",     "")) or
            str(row.get("اسم المنتج",     "")) or
            str(row.get("name",           "")) or ""
        ).strip()
        if name in ("", "nan", "None"): name = ""

        # ── السعر حسب القسم ───────────────────────────────────────────────
        comp_price = _safe_float(row.get("سعر_المنافس", 0))
        our_price  = _safe_float(
            row.get("السعر", 0) or row.get("سعر المنتج", 0) or
            row.get("price",  0) or 0
        )

        if section_type == "raise":
            # قسم "سعر أعلى": سعرنا أعلى من المنافس → نُخفّض إلى comp_price - 1
            # الشرط: يجب أن يكون لدينا سعر أصلي وأن الهدف فعلاً أقل منه
            _target = round(comp_price - 1, 2) if comp_price > 0 else our_price
            price = _target if (our_price > 0 and _target < our_price) else our_price
        elif section_type == "lower":
            # قسم "سعر أقل": سعرنا أقل من المنافس → نرفع إلى comp_price - 1 (نزيد الهامش)
            # الشرط: يجب أن يكون الهدف فعلاً أعلى من سعرنا الحالي
            _target = round(comp_price - 1, 2) if comp_price > 0 else our_price
            price = _target if (our_price > 0 and _target > our_price) else our_price
        elif section_type in ("approved", "update"):
            price = our_price
        else:
            # missing / new: سعر المنافس
            price = comp_price if comp_price > 0 else our_price

        if not name: continue

        # ── حقول سياقية إضافية ───────────────────────────────────────────
        comp_name  = str(row.get("منتج_المنافس", ""))
        comp_src   = str(row.get("المنافس", ""))
        diff       = _safe_float(row.get("الفرق", 0))
        match_pct  = _safe_float(row.get("نسبة_التطابق", 0))
        decision   = str(row.get("القرار", ""))
        brand      = str(row.get("الماركة", ""))

        product = {
            "product_id": product_id,
            "name":       name,
            "price":      float(price),
            "section":    section_type,
        }

        if comp_name and comp_name not in ("nan", "None", "—"):
            product["comp_name"] = comp_name
        if comp_src and comp_src not in ("nan", "None"):
            product["competitor"] = comp_src
        if diff:
            product["price_diff"] = diff
        if match_pct:
            product["match_score"] = match_pct
        if decision and decision not in ("nan", "None"):
            product["decision"] = decision
        if brand and brand not in ("nan", "None"):
            product["brand"] = brand

        products.append(product)

    return products


# ══════════════════════════════════════════════════════════════════════════
#  إرسال منتج واحد — تحديث السعر
#  Payload: {"products": [{"product_id":"...","name":"...","price":...}]}
# ══════════════════════════════════════════════════════════════════════════
def send_single_product(product: Dict) -> Dict:
    """
    إرسال منتج واحد لتحديث سعره في سلة عبر Make.
    Make يقرأ: {{2.products}} → product_id | name | price
    Payload: {"products": [{...}]}
    """
    if not product:
        return {"success": False, "message": "❌ لا توجد بيانات للإرسال"}

    name       = str(product.get("name", "")).strip()
    price      = _safe_float(product.get("price", 0))
    product_id = _clean_pid(product.get("product_id", ""))

    if not name:
        return {"success": False, "message": "❌ اسم المنتج مطلوب"}
    if price <= 0:
        return {"success": False, "message": f"❌ السعر غير صحيح: {price}"}
    if not product_id:
        return {
            "success": False,
            "message": f"❌ رقم المنتج (ID) مطلوب لتحديث سلة — «{name}» لا يملك رقماً. أضف عمود رقم المنتج في ملف كتالوجك.",
        }

    # ── Payload مطابق لما يقرأه Make: {{2.products}} ─────────────────────
    # Salla UpdateProduct يتوقع price كـ uinteger
    _prod = {
        "product_id":  product_id,
        "name":        name,
        "price":       int(round(price)),
        "section":     product.get("section", "update"),
        "comp_name":   product.get("comp_name", ""),
        "competitor":  product.get("competitor", ""),
        "price_diff":  product.get("price_diff", product.get("diff", 0)),
        "match_score": product.get("match_score", 0),
        "decision":    product.get("decision", ""),
        "brand":       product.get("brand", ""),
    }
    _cu = str(product.get("comp_url", product.get("رابط_المنافس", "")) or "").strip()
    if _cu:
        _prod["comp_url"] = _cu

    payload = {"products": [_prod]}

    result = _post_to_webhook(WEBHOOK_UPDATE_PRICES, payload)
    if result["success"]:
        pid_info = f" [ID: {product_id}]" if product_id else ""
        result["message"] = f"✅ تم تحديث «{name}»{pid_info} ← {price:,.0f} ر.س"
    return result


def trigger_price_update(
    sku: str,
    target_price: float,
    comp_url: str = "",
    *,
    name: str = "",
    comp_name: str = "",
    comp_price: float = 0.0,
    diff: float = 0.0,
    decision: str = "",
    competitor: str = "",
) -> bool:
    """
    غلاف تفاعلي لإرسال تحديث سعر واحد إلى Make.com.
    يعيد True عند نجاح HTTP — للاستخدام من أزرار الواجهة.
    """
    res = send_single_product({
        "product_id": sku,
        "name": name,
        "price": float(target_price),
        "comp_name": comp_name,
        "comp_price": comp_price,
        "diff": diff,
        "decision": decision,
        "competitor": competitor,
        "comp_url": comp_url or "",
    })
    return bool(res.get("success"))


# ══════════════════════════════════════════════════════════════════════════
#  إرسال عدة منتجات — تحديث الأسعار
#  Payload: {"products": [{product_id, name, price, ...}]}
#  Make يقرأ: {{2.products}} → BasicFeeder → UpdateProduct
# ══════════════════════════════════════════════════════════════════════════
def send_price_updates(products: List[Dict]) -> Dict:
    """
    إرسال قائمة منتجات لتحديث أسعارها في سلة عبر Make.
    Payload: {"products": [{product_id, name, price, ...}]}
    Make يقرأ {{2.products}} ويمرر كل عنصر لـ UpdateProduct.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}

    valid_products = []
    skipped = 0

    for p in products:
        name       = str(p.get("name", "")).strip()
        price      = _safe_float(p.get("price", 0))
        product_id = _clean_pid(p.get("product_id", ""))

        if not name or price <= 0 or not product_id:
            skipped += 1
            continue

        valid_products.append({
            "product_id":  product_id,
            "name":        name,
            "price":       int(round(price)),
            "section":     p.get("section", "update"),
            "comp_name":   p.get("comp_name", ""),
            "competitor":  p.get("competitor", ""),
            "price_diff":  p.get("price_diff", p.get("diff", 0)),
            "match_score": p.get("match_score", 0),
            "decision":    p.get("decision", ""),
            "brand":       p.get("brand", ""),
        })

    if not valid_products:
        return {
            "success": False,
            "message": f"❌ لا توجد منتجات صالحة (تم تخطي {skipped} منتج)"
        }

    # ── Payload مطابق لما يقرأه Make: {{2.products}} ─────────────────────
    payload = {"products": valid_products}
    result = _post_to_webhook(WEBHOOK_UPDATE_PRICES, payload)

    if result["success"]:
        skip_msg = f" (تم تخطي {skipped})" if skipped else ""
        result["message"] = f"✅ تم إرسال {len(valid_products)} منتج لتحديث الأسعار{skip_msg}"
    return result


# ══════════════════════════════════════════════════════════════════════════
#  إرسال منتجات جديدة — Webhook منفصل
#  Payload: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}
#  Make يقرأ: {{1.data}} → BasicFeeder → CreateProduct
# ══════════════════════════════════════════════════════════════════════════
def send_new_products(products: List[Dict]) -> Dict:
    """
    إرسال منتجات جديدة لإضافتها في سلة عبر Make.
    Payload: {"data": [{أسم المنتج, سعر المنتج, رمز المنتج sku, الوزن, ...}]}
    Make يقرأ {{1.data}} ويمرر كل عنصر لـ CreateProduct.
    يُرسل كل منتج في طلب مستقل.
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال"}

    sent, skipped, errors = 0, 0, []

    for p in products:
        name  = str(p.get("name", p.get("أسم المنتج", ""))).strip()
        price = _safe_float(
            p.get("price", 0) or p.get("سعر المنتج", 0) or p.get("السعر", 0)
        )
        pid   = _clean_pid(p.get("product_id", p.get("معرف_المنتج", "")))

        if not name:
            skipped += 1
            continue

        # ── بنية البيانات المطابقة لـ Interface سيناريو Make ─────────────
        # Salla CreateProduct يتوقع uinteger للأسعار والوزن → int(round(...))
        item = {
            "product_id":      pid,
            "أسم المنتج":      name,
            "سعر المنتج":      int(round(float(price))) if price else 0,
            "رمز المنتج sku":  str(p.get("sku", p.get("رمز المنتج sku", ""))).strip(),
            "الوزن":           max(1, int(round(_safe_float(p.get("weight", p.get("الوزن", 1))) or 1))),
            "سعر التكلفة":     int(round(_safe_float(p.get("cost_price", p.get("سعر التكلفة", 0))))),
            "السعر المخفض":    int(round(_safe_float(p.get("sale_price",  p.get("السعر المخفض", 0))))),
            "الوصف":           str(p.get("الوصف", p.get("description", ""))).strip(),
        }
        # صورة — فقط روابط صور مباشرة (بامتداد صورة) لا روابط بحث
        _raw_img = str(p.get("image_url", "") or "").strip()
        if _raw_img and any(ext in _raw_img.lower() for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
            item["صورة المنتج"] = _raw_img

        result = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, {"data": [item]})
        if result["success"]:
            sent += 1
        else:
            errors.append(name)

        if len(products) > 1:
            time.sleep(0.3)

    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات. تم تخطي {skipped}"}

    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج جديد إلى Make{skip_msg}{err_msg}"}


# ══════════════════════════════════════════════════════════════════════════
#  إرسال المنتجات المفقودة — نفس سيناريو المنتجات الجديدة
#  Payload: {"data": [{"أسم المنتج":"...","سعر المنتج":...,"الوصف":"..."}]}
# ══════════════════════════════════════════════════════════════════════════
def send_missing_products(products: List[Dict]) -> Dict:
    """
    إرسال المنتجات المفقودة لإضافتها في سلة عبر Make.
    يُستخدم نفس Webhook المنتجات الجديدة.
    Payload: {"data": [{أسم المنتج, سعر المنتج, ...}]}
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات مفقودة للإرسال"}

    sent, skipped, errors = 0, 0, []

    for p in products:
        name  = str(p.get("name", p.get("المنتج", p.get("منتج_المنافس", "")))).strip()
        price = _safe_float(
            p.get("price", 0) or p.get("السعر", 0) or p.get("سعر_المنافس", 0)
        )
        pid   = _clean_pid(p.get("product_id", p.get("معرف_المنتج", "")))

        if not name or price <= 0:
            skipped += 1
            continue

        # ── بنية البيانات المطابقة لـ Interface سيناريو Make ─────────────
        # Salla CreateProduct يتوقع uinteger للأسعار والوزن → int(round(...))
        item = {
            "product_id":      pid,
            "أسم المنتج":      name,
            "سعر المنتج":      int(round(float(price))),
            "رمز المنتج sku":  str(p.get("sku", p.get("رمز المنتج sku", ""))).strip(),
            "الوزن":           max(1, int(round(_safe_float(p.get("weight", p.get("الوزن", 1))) or 1))),
            "سعر التكلفة":     int(round(_safe_float(p.get("cost_price", p.get("سعر التكلفة", 0))))),
            "السعر المخفض":    int(round(_safe_float(p.get("sale_price",  p.get("السعر المخفض", 0))))),
            "الوصف":           str(p.get("الوصف", p.get("description", ""))).strip(),
        }
        # صورة — فقط روابط صور مباشرة (بامتداد صورة) لا روابط بحث
        _raw_img = str(p.get("image_url", "") or "").strip()
        if _raw_img and any(ext in _raw_img.lower() for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".avif")):
            item["صورة المنتج"] = _raw_img

        result = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, {"data": [item]})
        if result["success"]:
            sent += 1
        else:
            errors.append(name)

        if len(products) > 1:
            time.sleep(0.3)

    if sent == 0:
        return {"success": False, "message": f"❌ فشل إرسال جميع المنتجات المفقودة. تم تخطي {skipped}"}

    skip_msg = f" (تم تخطي {skipped})" if skipped else ""
    err_msg  = f" (فشل {len(errors)})" if errors else ""
    return {"success": True, "message": f"✅ تم إرسال {sent} منتج مفقود إلى Make{skip_msg}{err_msg}"}


# ══# ══════════════════════════════════════════════════════════════════════
#  إرسال بدفعات ذكية مع retry و progress callback
# ══════════════════════════════════════════════════════════════════════
def send_batch_smart(products: list, batch_type: str = "update",
                     batch_size: int = 20, max_retries: int = 3,
                     progress_cb=None, confidence_filter: str = "") -> Dict:
    """
    إرسال بدفعات ذكية مع retry تلقائي و progress callback.
    batch_type: "update" (تحديث أسعار) | "new" (منتجات جديدة/مفقودة)
    confidence_filter: "green" | "yellow" | "" (كل المستويات)
    progress_cb: callable(sent, failed, total, current_name)
    """
    if not products:
        return {"success": False, "message": "❌ لا توجد منتجات للإرسال",
                "sent": 0, "failed": 0, "total": 0, "errors": []}

    # فلترة حسب الثقة (للمفقودات)
    if confidence_filter:
        products = [p for p in products
                    if p.get("مستوى_الثقة", "green") == confidence_filter
                    or p.get("confidence_level", "green") == confidence_filter]

    total = len(products)
    if total == 0:
        return {"success": False, "message": "❌ لا توجد منتجات بهذا المستوى من الثقة",
                "sent": 0, "failed": 0, "total": 0, "errors": []}

    sent_count = 0
    fail_count = 0
    error_names = []

    # تقسيم لدفعات
    for i in range(0, total, batch_size):
        batch = products[i:i + batch_size]

        for attempt in range(1, max_retries + 1):
            try:
                if batch_type == "update":
                    result = send_price_updates(batch)
                else:
                    result = send_new_products(batch)

                if result["success"]:
                    sent_count += len(batch)
                    break
                elif attempt < max_retries:
                    time.sleep(2 * attempt)  # backoff
                    continue
                else:
                    fail_count += len(batch)
                    error_names.extend([p.get("name", p.get("منتج_المنافس", "?"))[:30] for p in batch])
            except Exception:
                if attempt >= max_retries:
                    fail_count += len(batch)
                    error_names.extend([p.get("name", "?")[:30] for p in batch])
                else:
                    time.sleep(2 * attempt)

        # progress callback
        if progress_cb:
            try:
                progress_cb(sent_count, fail_count, total,
                           batch[-1].get("name", "")[:30] if batch else "")
            except Exception:
                pass

        # تأخير بين الدفعات
        if i + batch_size < total:
            time.sleep(0.5)

    success = sent_count > 0
    msg_parts = []
    if sent_count > 0:
        msg_parts.append(f"✅ نجح {sent_count}")
    if fail_count > 0:
        msg_parts.append(f"❌ فشل {fail_count}")
    msg = f"إرسال {total} منتج: {' | '.join(msg_parts)}"

    return {
        "success":  success,
        "message":  msg,
        "sent":     sent_count,
        "failed":   fail_count,
        "total":    total,
        "errors":   error_names[:20],  # أول 20 خطأ فقط
    }


# ══════════════════════════════════════════════════════════════════════
#  فحص حالة الاتصال بـ Webhooks
# ══════════════════════════════════════════════════════════════════════════
def verify_webhook_connection() -> Dict:
    """
    فحص حالة الاتصال بجميع Webhooks.
    يُعيد dict: {"update_prices": {...}, "new_products": {...}, "all_connected": bool}
    """
    # فحص Webhook تحديث الأسعار — Payload المطابق للـ Parameters
    test_price_payload = {
        "products": [{
            "product_id": "test-001",
            "name":       "اختبار الاتصال",
            "price":      1,
            "section":    "test",
        }]
    }
    r1 = _post_to_webhook(WEBHOOK_UPDATE_PRICES, test_price_payload)

    # فحص Webhook المنتجات الجديدة
    test_new_payload = {
        "data": [{
            "product_id":     "",
            "أسم المنتج":     "اختبار الاتصال",
            "سعر المنتج":     1,
            "رمز المنتج sku": "TEST-SKU-001",
            "الوزن":          1,
            "سعر التكلفة":    0,
            "السعر المخفض":   0,
            "الوصف":          "اختبار تلقائي من نظام مهووس للتحقق من الاتصال",
        }]
    }
    r2 = _post_to_webhook(WEBHOOK_NEW_PRODUCTS, test_new_payload)

    return {
        "update_prices": {
            "success": r1["success"],
            "message": r1["message"],
            "url": WEBHOOK_UPDATE_PRICES[:55] + "..." if len(WEBHOOK_UPDATE_PRICES) > 55 else WEBHOOK_UPDATE_PRICES,
        },
        "new_products": {
            "success": r2["success"],
            "message": r2["message"],
            "url": WEBHOOK_NEW_PRODUCTS[:55] + "..." if len(WEBHOOK_NEW_PRODUCTS) > 55 else WEBHOOK_NEW_PRODUCTS,
        },
        "all_connected": r1["success"] and r2["success"],
    }
