"""
scripts/test_make_webhooks.py
اختبار الإرسال الفعلي لكلا Webhook في Make.com
الاستخدام:
  python scripts/test_make_webhooks.py <WEBHOOK_UPDATE_PRICES_URL> <WEBHOOK_NEW_PRODUCTS_URL>
  أو: ضع الـ URLs في متغيرات البيئة WEBHOOK_UPDATE_PRICES و WEBHOOK_NEW_PRODUCTS ثم شغّل بدون وسائط.
"""
import json
import os
import sys
import time
import requests

TIMEOUT = 15


def post(url: str, payload: dict, label: str) -> bool:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"URL: {url[:70]}...")
    print(f"Payload:\n{json.dumps(payload, ensure_ascii=False, indent=2)}")
    print()
    try:
        resp = requests.post(url, json=payload,
                             headers={"Content-Type": "application/json"},
                             timeout=TIMEOUT)
        if resp.status_code in (200, 201, 202, 204):
            print(f"✅  نجح! HTTP {resp.status_code}")
            if resp.text:
                print(f"   Response: {resp.text[:200]}")
            return True
        else:
            print(f"❌  فشل! HTTP {resp.status_code}")
            print(f"   Response: {resp.text[:300]}")
            return False
    except requests.exceptions.Timeout:
        print("❌  Timeout — لم يستجب Make خلال 15 ثانية")
        return False
    except Exception as e:
        print(f"❌  خطأ: {e}")
        return False


def main():
    # الحصول على الـ URLs
    if len(sys.argv) == 3:
        url_prices = sys.argv[1].strip()
        url_new    = sys.argv[2].strip()
    else:
        url_prices = os.environ.get("WEBHOOK_UPDATE_PRICES", "").strip()
        url_new    = os.environ.get("WEBHOOK_NEW_PRODUCTS",  "").strip()

    if not url_prices or not url_new:
        print("❌  يجب تمرير الـ Webhook URLs:")
        print("   python scripts/test_make_webhooks.py URL_UPDATE URL_NEW")
        print("   أو ضبط متغيرات WEBHOOK_UPDATE_PRICES و WEBHOOK_NEW_PRODUCTS")
        sys.exit(1)

    # ─── اختبار 1: سيناريو Integration Webhooks, Salla (تحديث الأسعار) ────
    # BasicFeeder يقرأ {{2.products}} → UpdateProduct: product_id | name | price(uinteger)
    price_payload = {
        "products": [
            {
                "product_id":  "TEST-ID-001",
                "name":        "اختبار تحديث السعر — Dior Sauvage EDP 100ml",
                "price":       299,           # uinteger ✓
                "section":     "test",
                "comp_name":   "Dior Sauvage EDP 100ml",
                "competitor":  "noon.com",
                "price_diff":  20,
                "match_score": 97,
                "decision":    "خفض السعر",
                "brand":       "Dior",
            }
        ]
    }

    ok1 = post(url_prices, price_payload, "سيناريو 1: Integration Webhooks, Salla — تحديث الأسعار")

    time.sleep(1)

    # ─── اختبار 2: سيناريو Mahwous إضافة منتجات جديدة لسلة ────────────────
    # BasicFeeder يقرأ {{1.data}} → CreateProduct
    # الحقول: أسم المنتج | سعر المنتج(uinteger) | رمز المنتج sku | الوزن(uinteger)
    #         سعر التكلفة(uinteger) | السعر المخفض(uinteger) | الوصف
    new_payload = {
        "data": [
            {
                "product_id":      "",
                "أسم المنتج":      "اختبار إضافة منتج — Lattafa Khamrah EDP 100ml",
                "سعر المنتج":      149,        # uinteger ✓
                "رمز المنتج sku":  "TEST-LAT-KHM-100",
                "الوزن":           1,           # uinteger ✓
                "سعر التكلفة":     95,          # uinteger ✓
                "السعر المخفض":    0,           # uinteger ✓
                "الوصف":           "اختبار تلقائي من نظام مهووس للتحقق من توافق تنسيق البيانات مع سيناريو Make.com",
            }
        ]
    }

    ok2 = post(url_new, new_payload, "سيناريو 2: Mahwous — إضافة منتجات جديدة لسلة")

    # ─── ملخص ────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  ملخص الاختبار")
    print(f"{'='*60}")
    print(f"  سيناريو تحديث الأسعار:   {'✅ نجح' if ok1 else '❌ فشل'}")
    print(f"  سيناريو المنتجات الجديدة: {'✅ نجح' if ok2 else '❌ فشل'}")
    print()
    if ok1 and ok2:
        print("🎉 كلا السيناريوهين يعملان بشكل صحيح!")
    else:
        print("⚠️  راجع أخطاء الاتصال أعلاه وتأكد من تفعيل السيناريو في Make.com")
    sys.exit(0 if (ok1 and ok2) else 1)


if __name__ == "__main__":
    main()
