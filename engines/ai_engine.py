"""
engines/ai_engine.py v26.0 — خبير مهووس الكامل
════════════════════════════════════════════════
✅ تسجيل الأخطاء الحقيقية (لا يبتلعها)
✅ تشخيص ذاتي لكل مزود AI
✅ خبير وصف منتجات مهووس الكامل (SEO + GEO)
✅ جلب صور المنتج من Fragrantica + Google
✅ بحث ويب DuckDuckGo + Gemini Grounding
✅ تحقق AI يُصحّح القسم الخاطئ
✅ تصنيف تلقائي لقسم "تحت المراجعة"
✅ v26.0: بحث أشمل في المتاجر السعودية مع تحليل JSON دقيق
"""
import base64
import hashlib
import logging
import requests, json, re, time, traceback
from config import GEMINI_API_KEYS, OPENROUTER_API_KEY, COHERE_API_KEY

_logger = logging.getLogger(__name__)

# قائمة النماذج بالأولوية — يتدرج تلقائياً إذا كان النموذج غير متاح (404)
_GEMINI_MODELS = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-pro",
]
_GM  = _GEMINI_MODELS[0]  # النموذج الافتراضي (للعرض في التشخيص)
_GU_TEMPLATE = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
_GU  = _GU_TEMPLATE.format(model=_GM)  # للتوافق مع الكود القديم
_OR  = "https://openrouter.ai/api/v1/chat/completions"
_CO  = "https://api.cohere.ai/v1/generate"

# ── سجل الأخطاء الأخيرة (يُعرض في صفحة التشخيص) ─────────────────────────
_LAST_ERRORS: list = []

def _log_err(source: str, msg: str):
    global _LAST_ERRORS
    entry = f"[{source}] {msg}"
    _LAST_ERRORS = ([entry] + _LAST_ERRORS)[:10]  # آخر 10 أخطاء

def get_last_errors() -> list:
    return _LAST_ERRORS.copy()


def _http_error_detail(r: requests.Response) -> str:
    """رسالة خطأ API خام للعرض في التشخيص (بدون إخفاء السبب)."""
    try:
        j = r.json()
        err = j.get("error")
        if isinstance(err, dict):
            return (err.get("message") or err.get("status") or str(err))[:400]
        if err:
            return str(err)[:400]
        return r.text[:300]
    except Exception:
        return r.text[:300]


def _build_diagnose_recommendations(results: dict) -> list:
    """توصيات تلقائية بناءً على نتائج التشخيص الفعلية."""
    rec = []
    gem = results.get("gemini") or []
    any_gem_ok = any("✅" in str(g.get("status", "")) for g in gem)
    any_429_g = any("429" in str(g.get("status_code", "")) or "429" in str(g.get("status", "")) for g in gem)
    any_403_g = any("403" in str(g.get("status_code", "")) or "403" in str(g.get("status", "")) for g in gem)
    or_res = str(results.get("openrouter", ""))
    co_res = str(results.get("cohere", ""))
    or_429 = "429" in or_res
    co_429 = "429" in co_res

    if any_429_g:
        rec.append(
            "Gemini (429 تجاوز الحد): انتظر 60–120 ثانية، أضف مفتاح API احتياطياً في الأسرار، "
            "أو خفّض معدل الطلبات. السلسلة في التطبيق تمرّ تلقائياً إلى OpenRouter ثم Cohere عند فشل Gemini."
        )
    if any_403_g:
        rec.append(
            "Gemini (403): المفتاح أو المنطقة قد تكون محظورة — تحقق من صلاحية المفتاح في Google AI Studio، "
            "أو جرّب شبكة/VPN مختلفة، أوفعّل OpenRouter كمسار بديل."
        )
    if not any_gem_ok and gem:
        rec.append(
            "لا يوجد مفتاح Gemini يعمل: راجع تفاصيل الخطأ تحت كل مفتاح؛ إن وُجد OpenRouter أو Cohere يعملان، "
            "سيستمر التطبيق باستخدامهما تلقائياً."
        )
    if or_429:
        rec.append("OpenRouter (429): انتظر قليلاً أو جرّب نموذجاً آخر؛ التطبيق يجرّب عدة نماذج مجانية بالتتابع.")
    if co_429:
        rec.append("Cohere (429): انتظر ثم أعد المحاولة؛ أو اعتمد على Gemini/OpenRouter إن كانا يعملان.")
    if not rec and (any_gem_ok or "✅" in or_res or "✅" in co_res):
        rec.append("جميع المسارات الأساسية سليمة نسبياً — احتفظ بمفتاح احتياطي لتفادي انقطاع التحليل عند الذروة.")
    return rec


# ── تشخيص شامل لجميع مزودي AI ─────────────────────────────────────────────
def diagnose_ai_providers() -> dict:
    """
    يختبر كل مزود ويُعيد تقريراً مفصلاً بالأخطاء الحقيقية.
    يُستدعى من صفحة الإعدادات.
    """
    results = {}

    # ── Gemini ────────────────────────────────────────────────────────────
    gemini_results = []
    for i, key in enumerate(GEMINI_API_KEYS or []):
        if not key:
            gemini_results.append({"key": i+1, "status": "❌ مفتاح فارغ", "status_code": None, "detail": ""})
            continue
        try:
            payload = {
                "contents": [{"parts": [{"text": "test"}]}],
                "generationConfig": {"maxOutputTokens": 5}
            }
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=15)
            detail = _http_error_detail(r) if r.status_code != 200 else ""
            base = {"key": i+1, "status_code": r.status_code, "detail": detail}
            if r.status_code == 200:
                gemini_results.append({**base, "status": "✅ يعمل"})
            elif r.status_code == 400:
                gemini_results.append({**base, "status": f"❌ 400 — {detail[:120] if detail else 'Bad Request'}"})
            elif r.status_code == 403:
                gemini_results.append({**base, "status": "❌ 403 — مفتاح غير مصرح أو IP محظور"})
            elif r.status_code == 429:
                gemini_results.append({**base, "status": f"⚠️ 429 — تجاوز الحد (Rate Limit){' — ' + detail[:120] if detail else ''}"})
            elif r.status_code == 404:
                gemini_results.append({**base, "status": f"❌ 404 — النموذج {_GM} غير متاح"})
            else:
                gemini_results.append({**base, "status": f"❌ {r.status_code} — {(detail or '')[:120]}"})
        except requests.exceptions.ConnectionError as e:
            gemini_results.append({"key": i+1, "status": f"❌ لا يوجد اتصال بالإنترنت أو جدار حماية: {str(e)[:60]}", "status_code": None, "detail": str(e)[:200]})
        except requests.exceptions.Timeout:
            gemini_results.append({"key": i+1, "status": "❌ انتهت المهلة (Timeout 15s)", "status_code": None, "detail": "timeout"})
        except Exception as e:
            gemini_results.append({"key": i+1, "status": f"❌ خطأ: {str(e)[:80]}", "status_code": None, "detail": str(e)[:200]})
    results["gemini"] = gemini_results

    # ── OpenRouter ────────────────────────────────────────────────────────
    if OPENROUTER_API_KEY:
        try:
            r = requests.post(_OR, json={
                "model": "google/gemini-2.0-flash",  # ← مستقر
                "messages": [{"role":"user","content":"test"}],
                "max_tokens": 5
            }, headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://mahwous.com"
            }, timeout=15)
            if r.status_code == 200:
                results["openrouter"] = "✅ يعمل"
            elif r.status_code == 401:
                results["openrouter"] = "❌ 401 — مفتاح OpenRouter غير صحيح"
            elif r.status_code == 402:
                results["openrouter"] = "❌ 402 — رصيد OpenRouter منتهٍ"
            elif r.status_code == 429:
                od = _http_error_detail(r)
                results["openrouter"] = f"⚠️ 429 — تجاوز الحد — {od[:120] if od else ''}"
            else:
                try: msg = r.json().get("error",{}).get("message","")
                except: msg = r.text[:100]
                od = _http_error_detail(r)
                results["openrouter"] = f"❌ {r.status_code} — {(od or msg)[:120]}"
        except requests.exceptions.ConnectionError:
            results["openrouter"] = "❌ لا اتصال بـ openrouter.ai — قد يكون محظوراً"
        except requests.exceptions.Timeout:
            results["openrouter"] = "❌ Timeout"
        except Exception as e:
            results["openrouter"] = f"❌ {str(e)[:80]}"
    else:
        results["openrouter"] = "⚠️ مفتاح غير موجود"

    # ── Cohere ────────────────────────────────────────────────────────────
    if COHERE_API_KEY:
        try:
            r = requests.post("https://api.cohere.com/v2/chat", json={
                "model": "command-a-03-2025",
                "messages": [{"role": "user", "content": "test"}],
            }, headers={
                "Authorization": f"Bearer {COHERE_API_KEY}",
                "Content-Type": "application/json",
            }, timeout=15)
            if r.status_code == 200:
                results["cohere"] = "✅ يعمل (command-a-03-2025)"
            elif r.status_code == 401:
                results["cohere"] = "❌ 401 — مفتاح Cohere غير صحيح"
            elif r.status_code == 402:
                results["cohere"] = "❌ 402 — رصيد Cohere منتهٍ"
            elif r.status_code == 429:
                d = _http_error_detail(r)
                results["cohere"] = f"⚠️ 429 — تجاوز الحد — {d[:100] if d else ''}"
            else:
                try: msg = r.json().get("message","")
                except: msg = r.text[:100]
                results["cohere"] = f"❌ {r.status_code} — {msg[:80]}"
        except requests.exceptions.ConnectionError:
            results["cohere"] = "❌ لا اتصال بـ api.cohere.com"
        except Exception as e:
            results["cohere"] = f"❌ {str(e)[:80]}"
    else:
        results["cohere"] = "⚠️ مفتاح غير موجود"

    results["recommendations"] = _build_diagnose_recommendations(results)
    return results


# ══ خبير وصف منتجات مهووس الكامل ══════════════════════════════════════════
MAHWOUS_EXPERT_SYSTEM = """أنت خبير عالمي في كتابة أوصاف منتجات العطور محسّنة لمحركات البحث التقليدية (Google SEO) ومحركات بحث الذكاء الصناعي (GEO/AIO). تعمل حصرياً لمتجر "مهووس" (Mahwous) - الوجهة الأولى للعطور الفاخرة في السعودية.---## مطابقة منطقية للمنتجات (إلزامية عند أي مقارنة أو سؤال عن «نفس العطر؟»)**تعريف SKU واحد (نفس المنتج التجاري):** نفس الماركة + نفس خط العطر + نفس **الحجم بالمل** + نفس **التركيز** (EDP / EDT / Parfum / Elixir / Cologne…).**قاعدة صارمة:** أي اختلاف في **الحجم (مثلاً 50 مل مقابل 100 مل)** أو في **التركيز** أو في **الخط** (مثلاً Sauvage مقابل Sauvage Elixir) = **منتجان مختلفان**؛ المطابقة المنطقية **0%** ولا يصح وصفهما ك«نفس العطر» حتى لو تطابق الاسم الظاهري.**أمثلة:** 50 مل ≠ 100 مل؛ **Sauvage EDP** ≠ **Sauvage Parfum**؛ إصدار Limited أو Collector لا يُعادل القياسي إلا إذا تطابقت التفاصيل صراحةً.**في FAQ أو أي تحليل:** إذا سُئلت عن تطابق منتجين، طبّق القواعد أعلاه قبل الإجابة ولا تخلط بين تركيزين أو حجمين مختلفين.---## هويتك ومهمتك**من أنت:**- خبير عطور محترف مع 15+ سنة خبرة في صناعة العطور الفاخرة- متخصص في SEO و Generative Engine Optimization (GEO)- كاتب محتوى عربي بارع بأسلوب راقٍ، ودود، عاطفي، وتسويقي مقنع- تمثل صوت متجر "مهووس" بكل احترافية وثقة**مهمتك:**كتابة أوصاف منتجات عطور شاملة، احترافية، ومحسّنة بشكل علمي صارم لتحقيق:1. تصدر نتائج البحث في Google (الصفحة الأولى)2. الظهور في إجابات محركات بحث الذكاء الصناعي (ChatGPT, Gemini, Perplexity)3. زيادة معدل التحويل (Conversion Rate) بنسبة 40-60%4. تعزيز ثقة العملاء (E-E-A-T: Experience, Expertise, Authoritativeness, Trustworthiness)---## القواعد العلمية الصارمة للكلمات المفتاحية### 1. هرمية الكلمات المفتاحية (إلزامية)**المستوى 1: الكلمة الرئيسية (Primary Keyword)**- الصيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"- مثال: "عطر أكوا دي بارما كولونيا إنتنسا أو دو كولون 180 مل للرجال"- التكرار: 5-7 مرات في وصف 1200 كلمة- الكثافة: 1.5-2%- المواقع الإلزامية:  * H1 (العنوان الرئيسي)  * أول 50 كلمة  * آخر 100 كلمة  * 2-3 عناوين فرعية  * قسم "لمسة خبير"**المستوى 2: الكلمات الثانوية (3 كلمات)**- أمثلة: "عطر رجالي خشبي"، "عطر فاخر ثابت"، "عطر رجالي للمكتب"- التكرار: 3-5 مرات لكل كلمة- الكثافة: 0.5-1% لكل كلمة- المواقع: العناوين الفرعية، النقاط النقطية، الفقرات الوصفية**المستوى 3: الكلمات الدلالية (LSI) (10-15 كلمة)**- الفئات:  * صفات: فاخر، راقٍ، أنيق، كلاسيكي، ثابت، فواح  * مكونات: برغموت، جلد، خشب الأرز، مسك، باتشولي  * أحاسيس: دافئ، منعش، حار، حمضي، ذكوري  * مناسبات: مكتب، رسمي، يومي، مساء، صيف، شتاء- التكرار: 2-3 مرات لكل كلمة- الكثافة: 0.3-0.5% لكل كلمة**المستوى 4: الكلمات الحوارية (5-8 عبارات)**- الأنماط:  * "أبحث عن عطر رجالي خشبي ثابت للعمل"  * "ما هو أفضل عطر رجالي حمضي للصيف"  * "هل يناسب [اسم العطر] الاستخدام اليومي"  * "الفرق بين EDC و EDP"- المواقع: FAQ، قسم "لمسة خبير"### 2. خريطة المواقع الاستراتيجية (إلزامية)**الأولوية القصوى (Critical Zones):****H1 (العنوان الرئيسي):**- يجب أن يطابق الكلمة الرئيسية 100%- صيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"**أول 100 كلمة (The Golden Paragraph):**- الكلمة الرئيسية في أول 50 كلمة- كلمة ثانوية واحدة على الأقل- 2-3 كلمات دلالية- أسلوب عاطفي جذاب- دعوة مبكرة للشراء- مثال: "قوة الحمضيات وعمق الجلد، توقيع خشبي فاخر للرجل الأنيق. عطر [الاسم الكامل] هو تحفة عطرية [جنسية الماركة] تجمع بين [مكون 1] و[مكون 2]. صدر عام [السنة] بتوقيع [المصمم]، ليمنحك حضوراً راقياً وثباتاً استثنائياً. هذا العطر [الجنس] الفاخر متوفر الآن حصرياً لدى مهووس بأفضل سعر. اشترِه الآن!"**العناوين الفرعية (H2/H3):**- 60% من العناوين يجب أن تحتوي على كلمات مفتاحية- أمثلة:  * "لماذا تختار عطر [الاسم] [الجنس]؟"  * "رحلة العطر: اكتشف الهرم العطري [العائلة العطرية] الفاخر"  * "متى وأين ترتدي هذا العطر [الجنس] الأنيق؟"  * "لمسة خبير من مهووس: تقييم احترافي لعطر [الاسم]"**النقاط النقطية:**- كل نقطة تبدأ بكلمة مفتاحية بولد- مثال: "**عطر رجالي خشبي فاخر:** يجمع بين..."**قسم FAQ:**- 6-8 أسئلة- كل سؤال = كلمة مفتاحية حوارية- الإجابة تكرر الكلمة المفتاحية مرة واحدة- الإجابة مفصلة (50-80 كلمة)**الفقرة الختامية (آخر 100 كلمة):**- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- دعوة قوية للشراء- تعزيز الثقة: "أصلي 100%"، "ضمان"، "آلاف العملاء"- الشعار: "عالمك العطري يبدأ من مهووس"---## بنية الوصف الإلزامية**الطول الإجمالي: 1200-1500 كلمة**### 1. الفقرة الافتتاحية (100-150 كلمة)- جملة افتتاحية عاطفية قوية- الكلمة الرئيسية كاملة في أول 50 كلمة- معلومات أساسية: الماركة، المصمم، سنة الإصدار، العائلة العطرية- دعوة مبكرة للشراء### 2. تفاصيل المنتج (نقاط نقطية)**العنوان:** "تفاصيل المنتج"- الماركة (مع رابط داخلي)- اسم العطر- المصمم/الموقّع- الجنس- العائلة العطرية- الحجم- التركيز- سنة الإصدار### 3. رحلة العطر: الهرم العطري (200-250 كلمة)**العنوان:** "رحلة العطر: اكتشف الهرم العطري [العائلة] الفاخر"- **النفحات العليا (Top Notes):** وصف حسي + المكونات- **النفحات الوسطى (Heart Notes):** وصف حسي + المكونات- **النفحات الأساسية (Base Notes):** وصف حسي + المكونات + معلومات الثبات**القاعدة:** استخدم لغة حسية عاطفية، ليس مجرد قائمة مكونات.### 4. لماذا تختار هذا العطر؟ (200-250 كلمة)**العنوان:** "لماذا تختار عطر [الاسم] [الجنس]؟"- 4-6 نقاط نقطية- كل نقطة تبدأ بكلمة مفتاحية بولد- تركز على الفوائد (Benefits) وليس الميزات (Features)- أمثلة:  * **توقيع عطري فريد:** ...  * **ثبات استثنائي طوال اليوم:** ...  * **حجم اقتصادي:** ...  * **مثالي للمكتب والمناسبات:** ...  * **عطر أصلي بسعر مميز:** ...### 5. متى وأين ترتدي هذا العطر؟ (150-200 كلمة) [جديد]**العنوان:** "متى وأين ترتدي عطر [الاسم] [الجنس]؟"- **الفصول المناسبة:** (مع تفسير)- **الأوقات المثالية:** (صباح، مساء، ليل)- **المناسبات:** (عمل، رسمي، كاجوال، رومانسي)- **الفئة العمرية:** (إن كان ذلك مناسباً)### 6. لمسة خبير من مهووس (200-250 كلمة) [إلزامي]**العنوان:** "لمسة خبير من مهووس: تقييمنا الاحترافي"- **الافتتاحية:** "بعد تجربتنا المعمقة لعطر [الاسم]، يمكننا القول بثقة..."- **التحليل الحسي:** وصف الافتتاحية، القلب، القاعدة من منظور الخبير- **الأداء:** الثبات (بالساعات)، الفوحان (ضعيف/متوسط/قوي)، الإسقاط- **المقارنات:** "إذا كنت من محبي [عطر مشابه 1] أو [عطر مشابه 2]، فإن [الاسم] سيكون..."- **التوصية:** "لمن نوصي به؟"- **نصيحة الخبير:** نصيحة عملية لأفضل استخدام**القاعدة:** استخدم ضمير "نحن"، اذكر تجربة فعلية، قدم نصيحة احترافية.### 7. الأسئلة الشائعة (FAQ) (250-300 كلمة)**العنوان:** "الأسئلة الشائعة حول عطر [الاسم]"- **6-8 أسئلة** (كل سؤال = كلمة مفتاحية حوارية)- أسئلة إلزامية:  1. "هل عطر [الاسم] مناسب للاستخدام اليومي في [المكان]؟"  2. "ما الفرق بين [التركيز الحالي] و[تركيز آخر]؟"  3. "ما هي مدة ثبات عطر [الاسم] على البشرة؟"  4. "هل يتوفر عطر [الاسم] كـ تستر؟"  5. "ما هو الفصل الأنسب لاستخدام عطر [الاسم]؟"  6. "هل عطر [الاسم] مناسب للمناسبات الرسمية؟"- أسئلة اختيارية:  7. "ما هي أفضل طريقة لرش عطر [الاسم] لأطول ثبات؟"  8. "هل يمكن دمج عطر [الاسم] مع عطور أخرى (Layering)؟"**القاعدة:** الإجابة 50-80 كلمة، تبدأ بـ "نعم/لا" عندما يكون مناسباً، تكرر الكلمة المفتاحية مرة واحدة.### 8. اكتشف أكثر من مهووس (100-120 كلمة)**العنوان:** "اكتشف المزيد من عطور [الجنس/الفئة]"- 3-5 روابط داخلية فعلية — استخدم نمط URLs مهووس التالي حرفياً:  * للرجال: https://mahwous.com/categories/men-perfumes  * للنساء: https://mahwous.com/categories/women-perfumes  * للجنسين: https://mahwous.com/categories/unisex-perfumes  * تستر: https://mahwous.com/categories/tester  * نيش: https://mahwous.com/categories/niche  * الماركة: https://mahwous.com/brands/[اسم-الماركة-بالإنجليزية-بالشرطة]- **رابط خارجي واحد** (إلزامي):  * "اقرأ المزيد عن عطر [الاسم] على [Fragrantica Arabia](https://www.fragranticarabia.com/?s=[اسم+العطر+بالإنجليزية])"- ⚠️ **ممنوع منعاً باتاً**: لا تستخدم google.com أو أي رابط بحث — استخدم mahwous.com حصراً.### 9. الفقرة الختامية (80-100 كلمة)**العنوان:** "عالمك العطري يبدأ من مهووس"- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- تعزيز الثقة: "أصلي 100%"، "ضمان الأصالة"، "توصيل سريع"، "آلاف العملاء الراضين"- دعوة قوية للشراء: "اطلب الآن"، "اشترِ الآن"- الشعار: "عالمك العطري يبدأ من مهووس"---## الأسلوب الكتابي (إلزامي)### المزيج المطلوب:1. **راقٍ ومحترف** (40%): لغة فصحى سليمة، مصطلحات عطرية دقيقة2. **ودود وقريب** (25%): خطاب مباشر بضمير "أنت"، أسلوب محادثة3. **عاطفي ورومانسي** (20%): أوصاف حسية، استحضار مشاعر ومشاهد4. **تسويقي ومقنع** (15%): دعوات للشراء، تعزيز الثقة، خلق حاجة### القواعد الأسلوبية:- **لا تستخدم الإيموجي** (غير احترافي)- **استخدم Bold** للكلمات المفتاحية المهمة (لا تبالغ)- **تجنب التكرار الممل:** استخدم مرادفات- **اكتب بطبيعية:** لا حشو للكلمات المفتاحية- **استخدم أرقام وإحصائيات:** "ثبات 7-9 ساعات"، "فوحان متوسط إلى قوي"---## التعامل مع المدخلات### إذا أعطاك المستخدم:**1. معلومات كاملة (الاسم، الماركة، الحجم، السعر، الروابط):**- اكتب الوصف مباشرة بدون أسئلة- استخدم المعلومات المقدمة- ابحث في Fragrantica Arabia عن باقي التفاصيل**2. معلومات ناقصة (فقط الاسم والماركة):**- ابحث في Fragrantica Arabia عن:  * المصمم  * سنة الإصدار  * العائلة العطرية  * الهرم العطري  * الحجم الأكثر مبيعاً (إذا لم يحدد المستخدم)- ابحث في Google عن السعر التقريبي في السوق السعودي- اكتب الوصف بناءً على ما وجدته**3. فقط اسم العطر (بدون ماركة):**- ابحث في Google و Fragrantica لتحديد الماركة- ثم اتبع الخطوة 2### مصادر البحث (بالترتيب):1. **Fragrantica Arabia** (https://www.fragranticarabia.com/) - المصدر الأساسي2. **Google Search** - للأسعار والمعلومات الإضافية3. **موقع الماركة الرسمي** - للمعلومات الدقيقة---## التنسيق النهائي (إلزامي)### المخرجات يجب أن تكون:1. **جاهزة للنسخ واللصق مباشرة** (بدون شرح أو تعليمات)2. **بصيغة Markdown** مع العناوين والتنسيق3. **منظمة بالترتيب المذكور أعلاه**4. **الروابط جاهزة** (إذا قدمها المستخدم)### لا ترسل:- ❌ "هذا هو الوصف..."- ❌ "يمكنك نسخ..."- ❌ "ملاحظة: ..."- ❌ أي تعليمات أو شرح بعد الوصف- ❌ روابط google.com أو أي روابط بحث وهمية### فقط أرسل:- ✅ الوصف الكامل جاهز للاستخدام- ✅ روابط mahwous.com الحقيقية في قسم "اكتشف المزيد"---## جدول التحقق النهائي (تحقق قبل الإرسال)قبل إرسال أي وصف، تأكد من:**الكلمات المفتاحية:**- [ ] الكلمة الرئيسية في H1- [ ] الكلمة الرئيسية في أول 50 كلمة- [ ] الكلمة الرئيسية في آخر 100 كلمة- [ ] الكلمة الرئيسية تكررت 5-7 مرات- [ ] 3 كلمات ثانوية (كل واحدة 3-5 مرات)- [ ] 10-15 كلمة دلالية (كل واحدة 2-3 مرات)- [ ] 5-8 عبارات حوارية في FAQ**البنية:**- [ ] الطول: 1200-1500 كلمة- [ ] 9 أقسام رئيسية (حسب البنية أعلاه)- [ ] قسم "لمسة خبير من مهووس" موجود- [ ] قسم "متى وأين ترتدي" موجود- [ ] FAQ يحتوي على 6-8 أسئلة- [ ] 3-5 روابط داخلية- [ ] 1 رابط خارجي (Fragrantica)**الأسلوب:**- [ ] مزيج: راقٍ + ودود + عاطفي + تسويقي- [ ] لا إيموجي- [ ] Bold للكلمات المهمة (بدون مبالغة)- [ ] 

## قواعد صارمة:
- اكتب باللغة العربية فقط
- الطول: 1200-1500 كلمة
- لا تختلق مكونات أو بيانات — ابنِ على الاسم فقط
- شخصيتك: الرجل الأنيق بالبدلة والغترة، خبير عطور متحمس
- لا تكتب JSON أو أكواد — نص مقروء فقط
"""

# أمثلة سياقية (few-shot) — مطابقة منطقية لمتجر مهووس (تُضاف لأنظمة التحقق والتصنيف)
MATCHING_FEW_SHOT_AR = """
### أمثلة تعليمية من سياق متجر مهووس (لا تنسخ الأسماء حرفياً في الإجابة — للمنطق فقط)

**مطابقة صحيحة (نفس SKU):**
- منتجنا: «ديور سوفاج أو دو تواليت 100 مل للرجال» | المنافس: «Dior Sauvage EDT 100ml Men» → تطابق الماركة + الخط + EDT + 100 مل.

**مطابقة خاطئة (0% — منتجان مختلفان):**
- «ديور سوفاج أو دو بارفان 100 مل» vs «Dior Sauvage Parfum 100ml» → يختلف التركيز (Parfum ≠ EDP) رغم تشابه الاسم.
- «لانكوم لافي إست بيل أو دو بارفان 50 مل» vs «Lancome La Vie Est Belle EDP 100ml» → يختلف الحجم (50 مقابل 100) **فالمطابقة 0%** حتى لو الاسم متطابق.

**قاعدة:** اختلاف **الحجم (مل)** أو **التركيز** أو **خط المنتج** (مثل Sauvage vs Sauvage Elixir) يعني **ليس نفس المنتج**.
"""

# ══ System Prompts للأقسام ══════════════════════════════════════════════════
PAGE_PROMPTS = {
"price_raise": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اعلى.
سعرنا اعلى من المنافس. قواعد: فرق<10 ابقاء | 10-30 مراجعة | >30 خفض فوري.
لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل الفرق مبرر؟ 3.السعر المقترح.
اجب بالعربية بايجاز واحترافية.""",
"price_lower": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اقل.
سعرنا اقل من المنافس = فرصة ربح ضائعة. فرق<10 ابقاء | 10-50 رفع تدريجي | >50 رفع فوري.
لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الامثل. اجب بالعربية بايجاز.""",
"approved": "انت خبير تسعير عطور. راجع المنتجات الموافق عليها وتاكد من استمرار صلاحيتها. اجب بالعربية.",
"missing": """انت خبير عطور فاخرة متخصص في المنتجات المفقودة بمتجر مهووس.
لكل منتج: 1.هل هو حقيقي وموثوق؟ 2.هل يستحق الاضافة؟ 3.السعر المقترح. 4.اولوية الاضافة (عالية/متوسطة/منخفضة). اجب بالعربية.""",
"review": MATCHING_FEW_SHOT_AR + """انت خبير تسعير عطور. هذه منتجات بمطابقة غير مؤكدة.
طبّق المطابقة المنطقية: إذا اختلف الحجم أو التركيز أو خط العطر فهما **ليسا** نفس المنتج (لا تعطِ «نعم»).
لكل منتج: هل هما نفس العطر فعلاً (نفس SKU)؟ نعم / لا / غير متأكد. اشرح السبب بالعربية.""",
"general": """انت مساعد ذكاء اصطناعي متخصص في تسعير العطور الفاخرة والسوق السعودي.
خبرتك: تحليل الاسعار، المنافسة، استراتيجيات التسعير، مكونات العطور.
اجب بالعربية باحترافية وايجاز يمكنك استخدام markdown.""",
"verify": MATCHING_FEW_SHOT_AR + """انت خبير تحقق من منتجات العطور دقيق جداً (متجر مهووس).

قواعد المطابقة المنطقية (إلزامية — لا استثناء):
- **match = false + confidence = 0** إذا اختلف أيٌّ مما يلي:
  * الحجم (مل): 50ml ≠ 100ml حتى لو الاسم متطابق تماماً.
  * التركيز: EDT ≠ EDP ≠ Parfum ≠ Extrait ≠ Elixir ≠ Body Mist ≠ Hair Mist.
  * خط العطر: Sauvage ≠ Sauvage Elixir ≠ Sauvage Parfum.
  * الماركة أو الجنس.
  * وجود كلمات (بديل/مستوحى/tester/تستر) في أحدهما دون الآخر.
- **match = false + confidence = 0** إذا كان الحجم موجوداً في أحدهما ومفقوداً في الآخر.
- **قاعدة ذهبية:** إذا لم تجد تطابقاً دقيقاً 100% في الحجم والتركيز والماركة والخط، لا تُخمّن. أرجع confidence = 0 وصنّف المنتج كـ "مفقود". التخمين أسوأ من الإقرار بعدم التطابق.
- **match = true** فقط عند: نفس الماركة + نفس الخط + نفس الحجم + نفس التركيز + الجنس متوافق.
- الحد الأدنى لقبول المطابقة: confidence ≥ 85. أي مطابقة بأقل من 85% = "مفقود".

تحقق من: الماركة + اسم المنتج + الحجم (ml) + النوع (EDP/EDT/Parfum…) + الجنس.
اجب JSON فقط بدون اي نص اضافي:
{"match":true/false,"confidence":0-100,"reason":"سبب واضح","correct_section":"احد الاقسام","suggested_price":0}""",
"market_search": """انت محلل اسعار عطور (السوق السعودي) تبحث في الانترنت.
اجب JSON فقط:
{"market_price":0,"price_range":{"min":0,"max":0},"competitors":[{"name":"","price":0}],"recommendation":"","confidence":0}""",
"reclassify": MATCHING_FEW_SHOT_AR + """انت نظام تصنيف دقيق لمنتجات العطور (متجر مهووس).

تعريف «نفس المنتج (SKU)»: نفس الماركة + نفس خط العطر + نفس الحجم (مل) + نفس التركيز.
أي اختلاف في الحجم أو التركيز أو الخط → ليس نفس المنتج → صنّف كـ "مفقود".

قواعد صارمة (لا استثناء):
- إذا أحد المنتجَين يحتوي (بديل/مستوحى/tester/تستر) والآخر لا → "مفقود" (confidence = 0).
- إذا الحجم موجود في أحدهما ومفقود في الآخر → "مفقود" (confidence = 0).
- إذا الحجمان مختلفان (مثل 50ml vs 100ml) → "مفقود" (confidence = 0).
- إذا التركيزان مختلفان (EDT ≠ EDP ≠ Parfum ≠ Extrait…) → "مفقود" (confidence = 0).
- الحد الأدنى للقبول كـ "نفس المنتج": confidence ≥ 85. أقل من ذلك → "مفقود".
- إذا لم تجد تطابقاً دقيقاً، لا تُخمّن — صنّف كـ "مفقود" مباشرة.

الأقسام الصحيحة:
- سعر اعلى: نفس المنتج (SKU) + سعرنا أعلى بأكثر من 10 ريال
- سعر اقل: نفس المنتج (SKU) + سعرنا أقل بأكثر من 10 ريال
- موافق:    نفس المنتج (SKU) + الفرق ≤ 10 ريال
- مفقود:   أي اختلاف في SKU أو confidence < 85

يجب أن يطابق idx الرقم داخل [1]،[2]،... في قائمة المدخلات (واحد لكل سطر مرسل).
اجب JSON فقط:
{"results":[{"idx":1,"section":"القسم","confidence":85,"match":true,"reason":""},...]}"""
}

# ══ استدعاءات AI ═══════════════════════════════════════════════════════════
def _call_gemini(prompt, system="", grounding=False, temperature=0.3, max_tokens=8192):
    full = f"{system}\n\n{prompt}" if system else prompt
    payload = {
        "contents": [{"parts": [{"text": full}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens, "topP": 0.85}
    }
    if grounding:
        payload["tools"] = [{"google_search": {}}]

    if not GEMINI_API_KEYS:
        _log_err("Gemini", "لا توجد مفاتيح API")
        return None

    for i, key in enumerate(GEMINI_API_KEYS):
        if not key:
            continue
        # يحاول كل نموذج بالتسلسل — يتخطى النموذج إذا كان غير متاح (404)
        for model in _GEMINI_MODELS:
            url = f"{_GU_TEMPLATE.format(model=model)}?key={key}"
            try:
                r = requests.post(url, json=payload, timeout=45)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("candidates"):
                        parts = data["candidates"][0]["content"]["parts"]
                        return "".join(p.get("text","") for p in parts)
                    else:
                        reason = data.get("promptFeedback",{}).get("blockReason","")
                        _log_err("Gemini", f"مفتاح {i+1}/{model}: لا نتائج — {reason}")
                    break  # نموذج يعمل لكن لا نتائج → لا فائدة من نماذج أخرى بنفس المفتاح
                elif r.status_code == 429:
                    _log_err("Gemini", f"مفتاح {i+1}/{model}: Rate Limit (429) — انتظار 2 ثانية")
                    time.sleep(2)
                    break  # نفس المفتاح محدود — انتقل للمفتاح التالي
                elif r.status_code == 403:
                    _log_err("Gemini", f"مفتاح {i+1}/{model}: IP محظور أو مفتاح غير مصرح (403)")
                    break  # المفتاح معطوب — انتقل للتالي
                elif r.status_code == 404:
                    _log_err("Gemini", f"مفتاح {i+1}/{model}: نموذج غير متاح (404) — جرب {model}")
                    continue  # جرب النموذج التالي في القائمة
                else:
                    try:
                        msg = r.json().get("error",{}).get("message","")
                    except Exception:
                        msg = r.text[:100]
                    _log_err("Gemini", f"مفتاح {i+1}/{model}: {r.status_code} — {msg[:80]}")
                    break
            except requests.exceptions.ConnectionError as e:
                _log_err("Gemini", f"مفتاح {i+1}: لا اتصال — {str(e)[:80]}")
                break
            except requests.exceptions.Timeout:
                _log_err("Gemini", f"مفتاح {i+1}/{model}: Timeout (45s)")
                break
            except Exception as e:
                _log_err("Gemini", f"مفتاح {i+1}/{model}: {str(e)[:80]}")
                break
    return None

def _call_openrouter(prompt, system=""):
    if not OPENROUTER_API_KEY:
        return None

    # نماذج مجانية صحيحة (محدَّثة مارس 2026)
    # نماذج مستقرة فقط — بدون النماذج التجريبية (exp)
    FREE_MODELS = [
        "meta-llama/llama-3.3-70b-instruct:free",
        "deepseek/deepseek-chat-v3-0324:free",
        "mistralai/mistral-7b-instruct:free",
        "qwen/qwen-2.5-72b-instruct:free",
        "google/gemma-3-27b-it:free",
    ]

    msgs = []
    if system:
        msgs.append({"role": "system", "content": system})
    msgs.append({"role": "user", "content": prompt})

    for model in FREE_MODELS:
        try:
            r = requests.post(_OR, json={
                "model": model,
                "messages": msgs,
                "temperature": 0.3,
                "max_tokens": 8192
            }, headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": "https://mahwous.com",
                "X-Title": "Mahwous"
            }, timeout=45)

            if r.status_code == 200:
                content = r.json()["choices"][0]["message"]["content"]
                if content and content.strip():
                    return content
            elif r.status_code == 429:
                _log_err("OpenRouter", f"{model}: Rate Limit (429) — انتظار 2 ثانية")
                time.sleep(2)  # ← 2 ثانية للـ 429
                continue
            elif r.status_code == 402:
                _log_err("OpenRouter", f"{model}: رصيد منتهٍ (402) — جرب النموذج التالي")
                continue
            elif r.status_code == 401:
                _log_err("OpenRouter", "مفتاح غير صحيح (401)")
                return None  # لا فائدة من تجربة نماذج أخرى
            else:
                try:
                    msg = r.json().get("error", {}).get("message", "")
                except Exception:
                    msg = r.text[:100]
                _log_err("OpenRouter", f"{model}: {r.status_code} — {msg[:80]}")
                continue

        except requests.exceptions.ConnectionError as e:
            _log_err("OpenRouter", f"لا اتصال — {str(e)[:80]}")
            return None  # إذا لا اتصال، لا فائدة من تجربة نماذج أخرى
        except requests.exceptions.Timeout:
            _log_err("OpenRouter", f"{model}: Timeout (45s)")
            continue
        except Exception as e:
            _log_err("OpenRouter", f"{model}: {str(e)[:80]}")
            continue

    return None

def _call_cohere(prompt, system=""):
    """
    Cohere — Fallback صامت فقط.
    أي خطأ (401/402/429/...) يُسجَّل ويُعاد None بدون إيقاف سير العمل.
    """
    if not COHERE_API_KEY:
        return None
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            "https://api.cohere.com/v2/chat",
            json={"model": "command-r-plus", "messages": messages, "temperature": 0.3},
            headers={"Authorization": f"Bearer {COHERE_API_KEY}",
                     "Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("message", {}).get("content", [{}])[0].get("text", "")
        elif r.status_code == 401:
            _log_err("Cohere", "مفتاح غير صحيح (401) — تجاوز Cohere")
            return None  # ← لا يوقف العمل، يمرر للـ fallback التالي
        elif r.status_code in (402, 403):
            _log_err("Cohere", f"غير مصرح ({r.status_code}) — تجاوز")
            return None
        elif r.status_code == 429:
            _log_err("Cohere", "Rate Limit (429) — انتظار 2 ثانية")
            time.sleep(2)
            return None
        else:
            try:   msg = r.json().get("message", "")
            except Exception: msg = r.text[:100]
            _log_err("Cohere", f"{r.status_code} — {msg[:80]}")
    except Exception as e:
        _log_err("Cohere", f"Fallback صامت — {str(e)[:60]}")
    return None

def _parse_json(txt):
    if not txt: return None
    try:
        clean = re.sub(r'```json|```','',txt).strip()
        s = clean.find('{'); e = clean.rfind('}')+1
        if s >= 0 and e > s:
            return json.loads(clean[s:e])
    except json.JSONDecodeError as _e:
        _logger.debug("_parse_json: JSONDecodeError — %s | نص[:200]=%s", _e, txt[:200])
    except Exception as _e:
        _logger.debug("_parse_json: خطأ غير متوقع — %s", _e)
    return None

def _search_ddg(query, num_results=5):
    """بحث DuckDuckGo مجاني"""
    try:
        r = requests.get("https://api.duckduckgo.com/", params={
            "q": query, "format": "json", "no_html": "1", "skip_disambig": "1"
        }, timeout=8)
        if r.status_code == 200:
            data = r.json()
            results = []
            if data.get("AbstractText"):
                results.append({"snippet": data["AbstractText"], "url": data.get("AbstractURL","")})
            for rel in data.get("RelatedTopics", [])[:num_results]:
                if isinstance(rel, dict) and rel.get("Text"):
                    results.append({"snippet": rel.get("Text",""), "url": rel.get("FirstURL","")})
            return results
    except: pass
    return []

def call_ai(prompt, page="general"):
    sys = PAGE_PROMPTS.get(page, PAGE_PROMPTS["general"])
    for fn, src in [
        (lambda: _call_gemini(prompt, sys), "Gemini"),
        (lambda: _call_openrouter(prompt, sys), "OpenRouter"),
        (lambda: _call_cohere(prompt, sys), "Cohere")
    ]:
        r = fn()
        if r: return {"success":True,"response":r,"source":src}
    return {"success":False,"response":"فشل الاتصال بجميع مزودي AI","source":"none"}

# ══ Gemini Chat ══════════════════════════════════════════════════════════════
def gemini_chat(message, history=None, system_extra=""):
    sys = PAGE_PROMPTS["general"]
    if system_extra:
        sys = f"{sys}\n\nسياق: {system_extra}"
    needs_web = any(k in message.lower() for k in ["سعر","price","كم","متوفر","يباع","market","سوق","الان","اليوم","حالي","اخر","جديد"])
    contents = []
    for h in (history or [])[-12:]:
        contents.append({"role":"user","parts":[{"text":h["user"]}]})
        contents.append({"role":"model","parts":[{"text":h["ai"]}]})
    contents.append({"role":"user","parts":[{"text":f"{sys}\n\n{message}"}]})
    payload = {"contents":contents,
               "generationConfig":{"temperature":0.4,"maxOutputTokens":4096,"topP":0.9}}
    if needs_web:
        payload["tools"] = [{"google_search":{}}]
    for key in GEMINI_API_KEYS:
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=40)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    text = "".join(p.get("text","") for p in parts)
                    return {"success":True,"response":text,
                            "source":"Gemini Flash" + (" + بحث ويب" if needs_web else "")}
            elif r.status_code == 429:
                time.sleep(1); continue
        except: continue
    r = _call_openrouter(message, sys)
    if r: return {"success":True,"response":r,"source":"OpenRouter"}
    return {"success":False,"response":"فشل الاتصال","source":"none"}

# ══ جلب صور المنتج من مصادر متعددة ══════════════════════════════════════════
def fetch_product_images(product_name, brand=""):
    """
    يجلب روابط صور المنتج من:
    1. Fragrantica Arabia (المصدر الأساسي)
    2. Google Images عبر Gemini Grounding
    3. DuckDuckGo كبديل
    يُرجع: {"images": [{"url":"...","source":"...","alt":"..."}], "fragrantica_url": "..."}
    """
    images = []
    fragrantica_url = ""

    # ── 1. Fragrantica Arabia (أفضل مصدر) ────────────────────────────────
    prompt_frag = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com وابحث أيضاً في fragrantica.com

أريد فقط:
1. رابط URL مباشر للصورة الرئيسية للعطر (يجب أن يكون رابط صورة حقيقي ينتهي بـ .jpg أو .png أو .webp)
2. روابط صور إضافية إذا وجدت (2-3 صور)
3. رابط صفحة المنتج على Fragrantica Arabia

أجب JSON فقط:
{{
  "main_image": "رابط URL الصورة الرئيسية المباشر",
  "extra_images": ["رابط2", "رابط3"],
  "fragrantica_url": "رابط الصفحة",
  "found": true/false
}}"""

    txt_frag = _call_gemini(prompt_frag, grounding=True)
    if txt_frag:
        data = _parse_json(txt_frag)
        if data and data.get("found") and data.get("main_image"):
            main = data["main_image"]
            if main and main.startswith("http") and any(ext in main.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                images.append({"url": main, "source": "Fragrantica Arabia", "alt": product_name})
            for extra in data.get("extra_images", []):
                if extra and extra.startswith("http") and len(images) < 4:
                    images.append({"url": extra, "source": "Fragrantica", "alt": product_name})
            fragrantica_url = data.get("fragrantica_url", "")

    # ── 2. Google Images عبر Gemini ───────────────────────────────────────
    if len(images) < 2:
        search_q = f"{product_name} {brand} perfume bottle official image site:sephora.com OR site:nocibé.fr OR site:parfumdreams.com"
        prompt_google = f"""ابحث عن صور المنتج: "{product_name}"
أريد روابط URL مباشرة لصور زجاجة العطر من المتاجر الرسمية مثل Sephora أو الموقع الرسمي للماركة.
الروابط يجب أن تنتهي بـ .jpg أو .png أو .webp وتكون صور حقيقية للمنتج.
أجب JSON: {{"images": ["رابط1","رابط2","رابط3"], "sources": ["مصدر1","مصدر2","مصدر3"]}}"""

        txt_google = _call_gemini(prompt_google, grounding=True)
        if txt_google:
            data2 = _parse_json(txt_google)
            if data2 and data2.get("images"):
                sources = data2.get("sources", [])
                for i, img_url in enumerate(data2["images"][:3]):
                    if img_url and img_url.startswith("http") and len(images) < 4:
                        src = sources[i] if i < len(sources) else "Google"
                        images.append({"url": img_url, "source": src, "alt": product_name})

    # ── 3. DuckDuckGo كبديل ───────────────────────────────────────────────
    if not images:
        ddg = _search_ddg(f"{product_name} perfume official image fragrantica")
        for r in ddg[:3]:
            url = r.get("url","")
            if url and any(ext in url.lower() for ext in [".jpg",".png",".webp"]):
                images.append({"url": url, "source": "DuckDuckGo", "alt": product_name})
                if len(images) >= 2: break

    # ── إذا لم نجد صور مباشرة، نُعيد قائمة فارغة (لا نُرجع روابط بحث) ──────
    # رابط البحث مثل fragranticarabia.com/?s=... ليس رابط صورة مباشراً
    # وسلة سترفض الملف إذا أدرجناه كصورة منتج

    return {
        "images": images,
        "fragrantica_url": fragrantica_url,
        "success": len(images) > 0
    }

# ══ جلب معلومات Fragrantica Arabia الكاملة ══════════════════════════════════
def fetch_fragrantica_info(product_name):
    """جلب صورة + مكونات + وصف من Fragrantica Arabia"""
    prompt = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com

احتاج:
1. رابط صورة المنتج المباشر (.jpg/.png/.webp)
2. مكونات العطر (top notes, middle notes, base notes)
3. وصف قصير بالعربية
4. الماركة والنوع (EDP/EDT) والحجم
5. رابط الصفحة

اجب JSON فقط:
{{
  "image_url": "رابط الصورة المباشر",
  "top_notes": ["مكون1","مكون2"],
  "middle_notes": ["مكون1","مكون2"],
  "base_notes": ["مكون1","مكون2"],
  "description_ar": "وصف قصير بالعربية",
  "brand": "",
  "type": "",
  "size": "",
  "year": "",
  "designer": "",
  "fragrance_family": "",
  "fragrantica_url": "رابط الصفحة"
}}"""

    txt = _call_gemini(prompt, grounding=True)
    if not txt: txt = _call_gemini(prompt)
    if not txt: return {"success":False}

    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":False,"description_ar":txt[:200] if txt else ""}


# ══ هوية مهووس + سلة — وصف شاعري سعودي (Gemini) ═══════════════════════════
MAHWOUS_SALLA_PROMPT = """أنت الآن خبير عطور سعودي محترف تعمل في متجر (مهووس - Mahwous).
أسلوبك: شاعري، واثق، دقيق تقنياً، ومقنع تسويقياً. لغتك العربية فصيحة بلمسة خليجية راقية.
المهمة: توليد وصف تفصيلي لمنتج عطر متوافق مع معايير SEO ومنصة سلة.

الهيكل الإلزامي للوصف (9 أقسام بالترتيب):
1. العنوان الجذاب (H2): [اسم العطر] + [الماركة] + [الحجم] + (التركيز).
2. المقدمة الشاعرية (100-150 كلمة): وصف حسي يلامس المشاعر + الكلمة المفتاحية في أول 50 كلمة + دعوة مبكرة للشراء.
3. تفاصيل المنتج: نقاط نقطية (الماركة، الجنس، العائلة العطرية، الحجم، التركيز، السنة).
4. الهرم العطري (H3): (النفحات العليا، الوسطى، الأساسية) — مكونات حقيقية فقط من المدخلات. لا تخترع.
5. لماذا تختار هذا العطر؟: 4-6 نقاط قوة (الثبات، الفوحان، التميز، القيمة).
6. متى وأين ترتديه؟: الفصول، الأوقات (صباح/مساء)، المناسبات.
7. لمسة خبير من مهووس (H3): تحليل حسي + الأداء (الثبات ساعات، الفوحان 1-10) + مقارنة بعطر مشابه + نصيحة رش.
8. اكتشف المزيد من مهووس (H3): استخدم الروابط المُعطاة في قسم [MAHWOUS_INTERNAL_LINKS] حرفياً كما هي — لا تبدّلها. ضعها كقائمة نقطية بـ anchor text وصفي.
9. الأسئلة الشائعة (H3): 6 أسئلة بإجابات 50-80 كلمة + الختام "عالمك العطري يبدأ من مهووس".

في نهاية الوصف، أضف بيانات SEO بصيغة JSON فقط (بدون markdown fence) كالتالي:
{"page_title":"...","meta_description":"...","url_slug":"...","alt_text":"...","tags":"..."}

قواعد صارمة:
- أكد أن العطر "أصلي 100%" مرة واحدة فقط.
- لا تخترع مكونات — استخدم ما في المدخلات أو اكتب "مزيج عطري فاخر".
- استخدم الروابط من [MAHWOUS_INTERNAL_LINKS] حرفياً — لا تستبدلها بـ Google أو أي روابط وهمية.
- لا تكتب أي شرح أو تعليمات بعد JSON — النص ينتهي عند آخر } في JSON.
- لا إيموجي. لا ملاحظات. لا مقدمات خارج الهيكل."""


def _parse_seo_json_block(text: str):
    """يفصل نص الوصف عن كتلة JSON النهائية (page_title / meta_description / …)."""
    if not text or not str(text).strip():
        return "", {}
    t = str(text).strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```\s*$", t)
    if m:
        try:
            j = json.loads(m.group(1).strip())
            if isinstance(j, dict) and any(k in j for k in ("page_title", "meta_description", "url_slug")):
                return t[: m.start()].strip(), j
        except Exception:
            pass
    last = t.rfind("\n{")
    if last == -1:
        last = t.rfind("{")
    if last != -1:
        tail = t[last:]
        depth = 0
        end = None
        for i, c in enumerate(tail):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end:
            try:
                j = json.loads(tail[:end])
                if isinstance(j, dict):
                    return t[:last].strip(), j
            except Exception:
                pass
    return t, {}


# ── أنماط روابط Google الوهمية التي يُولّدها النموذج ─────────────────────────
_FAKE_LINK_RE = re.compile(
    r"https?://(?:www\.)?google\.com/search\?[^\s\)\"'،,\u060c]*",
    re.I,
)

# رابط Fragrantica Arabia الأساسي
_FRAG_SEARCH_BASE = "https://www.fragranticarabia.com/?s="


def _load_mahwous_internal_links(
    brand: str = "",
    gender: str = "",
    ptype: str = "",
    product_name: str = "",
) -> str:
    """
    يُحمّل روابط متجر مهووس الداخلية من ملف Mahwoos_Internal_Links.txt إن وُجد،
    أو يُنشئها تلقائياً بناءً على الجنس ونوع المنتج والماركة.

    الأولوية:
    1. ملف data/Mahwoos_Internal_Links.txt (يرفعه صاحب المتجر يدوياً)
    2. بناء URLs تلقائي من categories + brands

    يُعيد block جاهزاً للحقن داخل الـ prompt.
    """
    import os as _os_local
    try:
        from utils.data_paths import get_catalog_data_path
        links_path = get_catalog_data_path("Mahwoos_Internal_Links.txt")
        if _os_local.path.exists(links_path):
            with open(links_path, "r", encoding="utf-8") as fh:
                content = fh.read().strip()
            if content and len(content) > 20:
                _logger.debug("_load_mahwous_internal_links: تحميل من ملف (%d حرف)", len(content))
                return content
    except Exception as _e:
        _logger.debug("_load_mahwous_internal_links: فشل تحميل الملف — %s", _e)

    # ── بناء URLs تلقائي ──────────────────────────────────────────────────
    gender_l  = (gender or product_name or "").lower()
    ptype_l   = (ptype or "").lower()

    # تحديد رابط القسم الرئيسي
    if any(kw in gender_l for kw in ("نساء", "نسائي", "للنساء", "women", "female", "femme")):
        cat_slug  = "women-perfumes"
        cat_label = "عطور نسائية فاخرة"
        tester_label = "تسترات نسائية"
    elif any(kw in gender_l for kw in ("رجال", "رجالي", "للرجال", "men", "male", "homme")):
        cat_slug  = "men-perfumes"
        cat_label = "عطور رجالية فاخرة"
        tester_label = "تسترات رجالية"
    else:
        cat_slug  = "unisex-perfumes"
        cat_label = "عطور للجنسين"
        tester_label = "تسترات عطور"

    # slug الماركة
    brand_clean = str(brand or "").strip()
    en_brand    = brand_clean
    if "|" in brand_clean:
        parts    = [p.strip() for p in brand_clean.split("|")]
        en_brand = next((p for p in parts if re.search(r"[a-zA-Z]", p)), parts[-1])
    brand_slug = re.sub(r"[^a-z0-9]+", "-", en_brand.lower().strip()).strip("-")

    # slug اسم المنتج لبحث Fragrantica
    frag_query = re.sub(r"\s+", "+", (product_name or brand_clean).strip()[:60])

    base = "https://mahwous.com"
    lines = [
        f"[{cat_label}]({base}/categories/{cat_slug})",
    ]
    if brand_slug and brand_clean:
        lines.append(f"[أفضل عطور {brand_clean} الأصلية]({base}/brands/{brand_slug})")
    lines += [
        f"[{tester_label} بأسعار مميزة]({base}/categories/tester)",
        f"[عطور النيش الحصرية]({base}/categories/niche)",
    ]
    if frag_query:
        lines.append(
            f"[تفاصيل العطر على Fragrantica Arabia]({_FRAG_SEARCH_BASE}{frag_query})"
        )

    return "\n".join(f"- {ln}" for ln in lines)


def _sanitize_desc_links(text: str, brand: str = "", gender: str = "", ptype: str = "", product_name: str = "") -> str:
    """
    يُنظّف الوصف المُولَّد من روابط Google الوهمية بخطوتين:

    الخطوة 1: إزالة كل URL يبدأ بـ google.com/search واستبداله برابط مهووس مناسب
              بناءً على نص الـ anchor المرفق (يُكتشف من Markdown).
    الخطوة 2: إزالة أي نص حواري ("أهلاً", "سؤال", "فهمت قصدك"…) يتسرب بعد JSON.
    """
    if not text:
        return text

    # ── استبدال روابط Google داخل Markdown [نص](url) ──────────────────────
    def _replace_link(m: re.Match) -> str:
        anchor = m.group(1) or ""
        url    = m.group(2) or ""
        # إذا الـ URL حقيقي (ليس Google/بحث) → أبقه كما هو
        if not _FAKE_LINK_RE.search(url):
            return m.group(0)

        anchor_l = anchor.lower()
        base     = "https://mahwous.com"
        brand_l  = (brand or "").lower()

        # بناء URL بدلالة الـ anchor
        if any(k in anchor_l for k in ("تستر", "tester")):
            new_url = f"{base}/categories/tester"
        elif any(k in anchor_l for k in ("نيش", "niche")):
            new_url = f"{base}/categories/niche"
        elif any(k in anchor_l for k in ("نساء", "نسائي", "women")):
            new_url = f"{base}/categories/women-perfumes"
        elif any(k in anchor_l for k in ("رجال", "رجالي", "men")):
            new_url = f"{base}/categories/men-perfumes"
        elif any(k in anchor_l for k in ("fragrantica",)):
            q = re.sub(r"\s+", "+", (product_name or anchor).strip()[:60])
            new_url = f"{_FRAG_SEARCH_BASE}{q}"
        elif brand_l and (brand_l in anchor_l or any(p.strip().lower() in anchor_l for p in brand.split("|"))):
            slug = re.sub(r"[^a-z0-9]+", "-", brand_l.split("|")[0].strip())[:30].strip("-")
            new_url = f"{base}/brands/{slug}"
        else:
            # بحث عام في متجر مهووس
            q = re.sub(r"\s+", "+", anchor.strip()[:50])
            new_url = f"{base}/search?q={q}"

        return f"[{anchor}]({new_url})"

    # استبدال كل [نص](رابط) يحتوي Google
    text = re.sub(
        r"\[([^\]]*)\]\((https?://[^\)]+)\)",
        _replace_link,
        text,
    )

    # ── روابط Google عارية بلا Markdown ──────────────────────────────────
    _fallback_q = re.sub(r"\s+", "+", (product_name or brand or "عطر").strip()[:40])
    text = _FAKE_LINK_RE.sub(f"https://mahwous.com/search?q={_fallback_q}", text)

    # ── قطع أي نص حواري يظهر بعد كتلة الوصف ────────────────────────────
    # أنماط تدل على أن النموذج بدأ يتحدث مع المستخدم بدل إكمال الوصف
    _CHAT_TRIGGER_RE = re.compile(
        r"\n+(?:أهلاً|مرحباً|بكل سرور|فهمت قصدك|العملية ستكون|"
        r"سؤال ممتاز|يمكنك الآن|يمكنك نسخ|ملاحظة:|note:|هذا هو الوصف)",
        re.I | re.UNICODE,
    )
    m = _CHAT_TRIGGER_RE.search(text)
    if m:
        text = text[: m.start()].rstrip()

    return text


def auto_infer_category(product_name: str, gender_hint: str = "") -> str:
    """مسار تصنيف سلة تلقائي من الاسم والجنس."""
    s = f"{product_name} {gender_hint}".lower()
    if any(x in s for x in ("نسائي", "نساء", "للنساء", "women", "female", "lady")):
        return "العطور > عطور نسائية"
    if any(x in s for x in ("رجالي", "رجال", "للرجال", "men", "homme", "male")):
        return "العطور > عطور رجالية"
    if any(x in s for x in ("للجنسين", "unisex", "الجنسين")):
        return "العطور > عطور للجنسين"
    return "العطور > عطور رجالية"


# ══ خبير وصف مهووس — توليد وصف سلة + SEO ═══════════════════════════════════
def generate_mahwous_description(
    product_name,
    price,
    fragrantica_data=None,
    extra_info=None,
    return_seo=False,
    brand: str = "",
    gender: str = "",
    ptype_hint: str = "",
):
    """
    يولّد وصفاً بلهجة سلة الشامل (9 أقسام، شاعري، سعودي) + JSON SEO في النهاية.

    الجديد v2:
    - يحقن روابط مهووس الداخلية الحقيقية بدلاً من تركها للنموذج يخترعها.
    - يُنظّف أي روابط Google وهمية تتسرب من النموذج.
    - يقطع أي نص حواري يظهر بعد كتلة JSON.

    المعاملات الجديدة:
        brand      : اسم الماركة (عربي | English) لبناء رابط صفحة الماركة.
        gender     : جنس العطر (رجالي/نسائي/للجنسين) لتحديد قسم التصفح.
        ptype_hint : نوع العطر (EDT/EDP…) للسياق.
    """
    frag_info = ""
    _brand  = brand or ""
    _gender = gender or ""
    _ptype  = ptype_hint or ""

    if fragrantica_data and fragrantica_data.get("success"):
        top      = ", ".join(fragrantica_data.get("top_notes",    [])[:5])
        mid      = ", ".join(fragrantica_data.get("middle_notes", [])[:5])
        base_n   = ", ".join(fragrantica_data.get("base_notes",   [])[:5])
        desc     = fragrantica_data.get("description_ar", "")
        _brand   = _brand  or fragrantica_data.get("brand",             "")
        _ptype   = _ptype  or fragrantica_data.get("type",              "")
        size     = fragrantica_data.get("size",             "")
        year     = fragrantica_data.get("year",             "")
        designer = fragrantica_data.get("designer",         "")
        family   = fragrantica_data.get("fragrance_family", "")
        frag_url = fragrantica_data.get("fragrantica_url",  "")

        frag_info = f"""
معلومات من Fragrantica Arabia (استخدمها فقط — لا تختلق غيرها):
- الماركة: {_brand}
- المصمم: {designer}
- سنة الإصدار: {year}
- العائلة العطرية: {family}
- الحجم: {size}
- التركيز: {_ptype}
- النفحات العليا: {top}
- النفحات الوسطى: {mid}
- النفحات الأساسية: {base_n}
- الوصف المرجعي: {desc}
- رابط Fragrantica: {frag_url}"""

    # ── بناء الروابط الداخلية الحقيقية ──────────────────────────────────
    internal_links = _load_mahwous_internal_links(
        brand=_brand,
        gender=_gender or product_name,
        ptype=_ptype,
        product_name=product_name,
    )

    extra = f"\nمعلومات إضافية: {extra_info}" if extra_info else ""

    prompt = f"""اكتب وصفاً كاملاً من 9 أقسام وفق الهيكل الإلزامي في التعليمات.

**اسم المنتج:** {product_name}
**السعر المرجعي للبيع:** {price:.0f} ريال سعودي
{frag_info}{extra}

[MAHWOUS_INTERNAL_LINKS]
استخدم الروابط التالية حرفياً في القسم 8 — لا تبدّلها ولا تضف روابط أخرى:
{internal_links}
[/MAHWOUS_INTERNAL_LINKS]

الطول: 1000–1500 كلمة، Markdown، بدون إيموجي.
أنهِ النص بـ JSON فقط (page_title, meta_description, url_slug, alt_text, tags) دون أي نص بعده."""

    txt = _call_gemini(prompt, MAHWOUS_SALLA_PROMPT, grounding=not bool(frag_info), max_tokens=8192)
    if not txt:
        txt = _call_gemini(prompt, MAHWOUS_SALLA_PROMPT, grounding=False, max_tokens=8192)
    if not txt:
        txt = _call_openrouter(prompt, MAHWOUS_SALLA_PROMPT)
    if not txt:
        txt = _call_cohere(prompt, MAHWOUS_SALLA_PROMPT)

    if not txt:
        fb = (
            f"## {product_name}\n\nعطر أصلي 100% متوفر في مهووس.\n\n**السعر:** {price:.0f} ر.س\n\n"
            f'{{"page_title":"{product_name[:80]}","meta_description":"عطر أصلي من مهووس","url_slug":"","alt_text":"","tags":"عطور"}}'
        )
        body, seo = _parse_seo_json_block(fb)
        if return_seo:
            return {"body": body, "seo": seo, "raw": fb}
        return body

    # ── تنظيف الروابط الوهمية + النصوص الحوارية ─────────────────────────
    txt = _sanitize_desc_links(txt, brand=_brand, gender=_gender, ptype=_ptype, product_name=product_name)

    body, seo = _parse_seo_json_block(txt)
    if return_seo:
        return {"body": body, "seo": seo, "raw": txt}
    return body if body else txt

# ══ تحقق منتج + تحديد القسم الصحيح ════════════════════════════════════════
# ══════════════════════════════════════════════════════════════════════════════
#  verify_perfume_match — طبقة تحقق Python صارمة (لا تعتمد على AI)
#  تُطبَّق قبل كل استدعاء AI وبعده — تمنع المطابقات الكارثية (EDT vs Parfum)
# ══════════════════════════════════════════════════════════════════════════════

# ── استخراج الحجم ────────────────────────────────────────────────────────────
_VM_SIZE_RE = re.compile(
    r'(\d+(?:\.\d+)?)\s*(?:ml|مل|ML|Ml|cl|fl\.?\s*oz)',
    re.I | re.UNICODE,
)

# ── استخراج التركيز (مُرتَّب من الأقوى إلى الأضعف) ──────────────────────────
# ⚠️  الترتيب مهم: EDP قبل PARFUM حتى لا تخطف "بارفيوم" وحدها من "أو دو بارفيوم"
_VM_CONC_PATTERNS = [
    # EXTRAIT — الأقوى
    (re.compile(
        r'\b(?:extrait\s+de\s+parfum|extrait|اكستريه|اكسترايت|اكسترا\s+دو\s+بارفيوم)\b',
        re.I | re.UNICODE), "EXTRAIT"),

    # EDP — Eau de Parfum (يجب أن يأتي قبل PARFUM)
    (re.compile(
        r'\beau\s+de\s+parfum\b'
        r'|\be\.?d\.?p\b'
        r'|أو\s+دو\s+بارفيوم|او\s+دو\s+بارفيوم'
        r'|\bأو\s+بارفيوم\b|\bاو\s+بارفيوم\b',
        re.I | re.UNICODE), "EDP"),

    # EDT — Eau de Toilette
    (re.compile(
        r'\beau\s+de\s+toilette\b'
        r'|\be\.?d\.?t\b'
        r'|أو\s+دو\s+تواليت|او\s+دو\s+تواليت'
        r'|\bتواليت\b',
        re.I | re.UNICODE), "EDT"),

    # EDC — Eau de Cologne
    (re.compile(
        r'\beau\s+de\s+cologne\b'
        r'|\be\.?d\.?c\b'
        r'|أو\s+دو\s+كولون|او\s+دو\s+كولون'
        r'|\bكولون\b(?!\s+إنتنس)|\bcologne\b',
        re.I | re.UNICODE), "EDC"),

    # PARFUM وحده — يأتي بعد EDP حتى لا يستأثر بـ "أو دو بارفيوم"
    (re.compile(
        r'(?<!\bde\s)(?<!\bدو\s)(?<!\bدو\s)\bparfum\b'
        r'|\bبارفيوم\b(?!\s+كولون)(?!\s+او)(?!\s+أو)'
        r'|\bبارفيوم\s+(?!كولون|او|أو)'
        r'|\bبارفان\b',
        re.I | re.UNICODE), "PARFUM"),

    # ELIXIR — خط منتج (ليس مجرد تركيز)
    (re.compile(
        r'\belixir\b|اليكسير|الكسير',
        re.I | re.UNICODE), "ELIXIR"),

    # BODY MIST / SPRAY — تختلف اختلافاً جوهرياً عن العطر
    (re.compile(
        r'\bbody\s*mist\b|\bbody\s*spray\b|\bbody\s*lotion\b'
        r'|بادي\s*ميست|بادي\s*سبراي|بودي\s*ميست',
        re.I | re.UNICODE), "BODY_MIST"),

    # HAIR MIST / SPRAY
    (re.compile(
        r'\bhair\s*mist\b|\bhair\s*spray\b|\bhair\s*perfume\b'
        r'|هير\s*ميست|هير\s*سبراي',
        re.I | re.UNICODE), "HAIR_MIST"),
]

# أزواج التركيز غير المتوافقة — أي منها → رفض صارم (0% match)
_VM_INCOMPATIBLE: frozenset = frozenset([
    frozenset(["EDP",     "EDT"]),
    frozenset(["EDP",     "EDC"]),
    frozenset(["EDP",     "PARFUM"]),
    frozenset(["EDT",     "PARFUM"]),
    frozenset(["EDT",     "EDC"]),
    frozenset(["EXTRAIT", "EDP"]),
    frozenset(["EXTRAIT", "EDT"]),
    frozenset(["EXTRAIT", "EDC"]),
    frozenset(["EXTRAIT", "PARFUM"]),
    frozenset(["ELIXIR",  "EDP"]),
    frozenset(["ELIXIR",  "EDT"]),
    frozenset(["ELIXIR",  "PARFUM"]),
    frozenset(["BODY_MIST", "EDP"]),
    frozenset(["BODY_MIST", "EDT"]),
    frozenset(["BODY_MIST", "PARFUM"]),
    frozenset(["BODY_MIST", "EXTRAIT"]),
    frozenset(["HAIR_MIST", "EDP"]),
    frozenset(["HAIR_MIST", "EDT"]),
    frozenset(["HAIR_MIST", "PARFUM"]),
    frozenset(["HAIR_MIST", "EXTRAIT"]),
])

# فارق الحجم المسموح به (ml) قبل الرفض
_VM_SIZE_TOLERANCE_ML: float = 10.0

# ── قاعدة الكلمات المحظورة (Blacklist) ──────────────────────────────────────
# أحدهما بديل/تستر والآخر أصلي = رفض قاطع بغض النظر عن باقي البيانات
_VM_BLACKLIST_RE = re.compile(
    r'\b(?:بديل|بدائل|مستوحى|مستوحاة|inspired\s+by|alternative|'
    r'tester|تستر|تسترز|testers)\b',
    re.I | re.UNICODE,
)


def _vm_extract_size(name: str) -> float | None:
    """يستخرج أول حجم (ml) من الاسم. يُرجع None إذا لم يجد."""
    m = _VM_SIZE_RE.search(str(name or ""))
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _vm_extract_conc(name: str) -> str | None:
    """
    يستخرج التركيز العطري من الاسم.
    يُرجع أول تطابق من _VM_CONC_PATTERNS (مُرتَّبة: EXTRAIT → EDP → EDT → …).
    """
    s = str(name or "")
    for pattern, conc in _VM_CONC_PATTERNS:
        if pattern.search(s):
            return conc
    return None


def verify_perfume_match(name1: str, name2: str) -> dict:
    """
    طبقة تحقق Python صارمة (Hard Rules) — تعمل قبل AI وبعده.

    قواعد الرفض الصارمة (بالأولوية):
    ──────────────────────────────────────────────────────────
    0. Blacklist: إذا أحدهما يحتوي (بديل/مستوحى/tester/تستر) والآخر لا → REJECT.
       المنتج الأصلي لا يطابق بديلاً أو تستراً أبداً.

    1. عدم تماثل الحجم: إذا كان الحجم موجوداً في أحدهما ومفقوداً في الآخر → REJECT.
       مثال: "سوفاج 100مل" vs "Sauvage EDP" → مرفوض (لا تماثل).

    2. اختلاف الحجم: إذا كلاهما له حجم والفارق > 10ml → REJECT.
       مثال: 200ml vs 100ml → فارق 100ml → مرفوض.

    3. التركيز: إذا تعرّف النظام على تركيز مختلف في الاسمَين → REJECT.
       مثال: EDT vs Parfum | EDP vs EDT | Elixir vs EDP → مرفوضة.

    إذا لم يُستخرج أي من الجانبَين → لا قرار (ok=True) والـ AI يحكم.
    ──────────────────────────────────────────────────────────
    Returns:
        {
          "ok":     bool,    # True=اجتاز / False=رفض صارم
          "reason": str,     # سبب الرفض (فارغ إذا ok=True)
          "size1":  float|None,
          "size2":  float|None,
          "conc1":  str|None,
          "conc2":  str|None,
        }
    """
    n1 = str(name1 or "").strip()
    n2 = str(name2 or "").strip()

    size1 = _vm_extract_size(n1)
    size2 = _vm_extract_size(n2)
    conc1 = _vm_extract_conc(n1)
    conc2 = _vm_extract_conc(n2)

    # ── قاعدة 0: Blacklist — بديل/تستر vs أصلي ───────────────────────────
    bl1 = bool(_VM_BLACKLIST_RE.search(n1))
    bl2 = bool(_VM_BLACKLIST_RE.search(n2))
    if bl1 != bl2:
        tag = "بديل/مستوحى" if ("بديل" in n1.lower() or "بديل" in n2.lower()
                                  or "مستوح" in n1.lower() or "مستوح" in n2.lower()
                                  or "inspired" in n1.lower() or "inspired" in n2.lower()) else "تستر"
        return {
            "ok":     False,
            "reason": (
                f"[Blacklist] أحد المنتجَين يحتوي كلمة محظورة ({tag}) "
                f"والآخر لا — مطابقة أصلي ببديل/تستر مرفوضة"
            ),
            "size1": size1, "size2": size2,
            "conc1": conc1, "conc2": conc2,
        }

    # ── قاعدة 1: عدم تماثل الحجم ─────────────────────────────────────────
    if (size1 is None) != (size2 is None):
        has_size = size1 if size1 is not None else size2
        return {
            "ok":     False,
            "reason": (
                f"[Volume Asymmetry] حجم موجود ({has_size:.0f}ml) في أحد المنتجَين "
                f"ومفقود في الآخر — مطابقة 0%"
            ),
            "size1": size1, "size2": size2,
            "conc1": conc1, "conc2": conc2,
        }

    # ── قاعدة 2: اختلاف الحجم ────────────────────────────────────────────
    if size1 is not None and size2 is not None:
        diff = abs(size1 - size2)
        if diff > _VM_SIZE_TOLERANCE_ML:
            return {
                "ok":     False,
                "reason": (
                    f"[Volume Mismatch] {size1:.0f}ml ≠ {size2:.0f}ml "
                    f"(فارق {diff:.0f}ml > {_VM_SIZE_TOLERANCE_ML:.0f}ml)"
                ),
                "size1": size1, "size2": size2,
                "conc1": conc1, "conc2": conc2,
            }

    # ── قاعدة 3: التركيز ──────────────────────────────────────────────────
    if conc1 and conc2 and conc1 != conc2:
        pair = frozenset([conc1, conc2])
        if pair in _VM_INCOMPATIBLE:
            return {
                "ok":     False,
                "reason": (
                    f"[Concentration Mismatch] {conc1} ≠ {conc2} — مطابقة مستحيلة"
                ),
                "size1": size1, "size2": size2,
                "conc1": conc1, "conc2": conc2,
            }

    return {
        "ok":     True,
        "reason": "",
        "size1":  size1, "size2":  size2,
        "conc1":  conc1, "conc2":  conc2,
    }


def verify_match(p1, p2, pr1=0, pr2=0):
    # ── مرحلة 0: الفحص الصارم بالـ Python قبل أي استدعاء AI ─────────────────
    _vmr = verify_perfume_match(p1, p2)
    if not _vmr["ok"]:
        # رفض صارم — لا حاجة للـ AI، التركيز أو الحجم مختلف
        _logger.info(
            "verify_match HARD-REJECT: «%s» vs «%s» — %s",
            p1[:60], p2[:60], _vmr["reason"],
        )
        return {
            "success":        True,
            "match":          False,
            "confidence":     0,
            "reason":         _vmr["reason"],
            "correct_section": "مفقود",
            "suggested_price": 0,
            "hard_reject":    True,
            "conc1":          _vmr.get("conc1"),
            "conc2":          _vmr.get("conc2"),
            "size1":          _vmr.get("size1"),
            "size2":          _vmr.get("size2"),
        }

    # ── مرحلة 1: AI ───────────────────────────────────────────────────────────
    diff = pr1 - pr2 if pr1 > 0 and pr2 > 0 else 0
    if pr1 > 0 and pr2 > 0:
        if diff > 10:     expected = "سعر اعلى"
        elif diff < -10:  expected = "سعر اقل"
        else:             expected = "موافق"
    else:
        expected = "تحت المراجعة"

    # أضف معلومات الحجم والتركيز المستخرجة في الـ prompt للمساعدة
    _ctx = ""
    if _vmr.get("conc1") or _vmr.get("size1"):
        _ctx = (
            f"\n[تحليل Python مسبق] منتج 1: حجم={_vmr.get('size1') or '?'}ml "
            f"/ تركيز={_vmr.get('conc1') or '؟'} | "
            f"منتج 2: حجم={_vmr.get('size2') or '?'}ml "
            f"/ تركيز={_vmr.get('conc2') or '؟'}"
        )

    prompt = f"""تحقق من تطابق هذين المنتجين بدقة متناهية (99.9%):
منتج 1 (مهووس): {p1} | السعر: {pr1:.0f} ريال
منتج 2 (المنافس): {p2} | السعر: {pr2:.0f} ريال{_ctx}

قواعد الرفض الصارمة (لا استثناء):
1. الماركة متطابقة تماماً.
2. خط العطر متطابق (Sauvage ≠ Sauvage Elixir ≠ Sauvage Elixir Parfum).
3. الحجم بالمل متطابق — **50ml مقابل 100ml = مطابقة 0%** حتى لو تطابق الاسم.
4. التركيز متطابق — EDT ≠ EDP ≠ Parfum ≠ Extrait ≠ Elixir ≠ Body Mist.
   مثال صارم: «إيروس أو دو تواليت 200ml» ≠ «إيروس بارفيوم» (EDT vs Parfum).
5. الجنس متطابق (Men ≠ Women).

إذا تعذّر تحقق أي شرط أعلاه، فالمطابقة **false** وconfidence = 0.

إذا كانت كل الشروط متوفرة، أجب بـ:
- القسم الصحيح = {expected}
خلاف ذلك:
- القسم الصحيح = مفقود"""

    sys = PAGE_PROMPTS["verify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt:
        return {"success":False,"match":False,"confidence":0,"reason":"فشل AI","correct_section":"تحت المراجعة","suggested_price":0}
    data = _parse_json(txt)
    if data:
        # ── مرحلة 2: فحص Python بعد AI — لا تقبل "match=true" إذا اختلف التركيز/الحجم
        if data.get("match") is True:
            _post = verify_perfume_match(p1, p2)
            if not _post["ok"]:
                data["match"]          = False
                data["confidence"]     = 0
                data["reason"]         = f"[Python Override] {_post['reason']}"
                data["correct_section"] = "مفقود"
                data["hard_reject"]    = True
                return {"success": True, **data}
        sec = data.get("correct_section","")
        if "اعلى" in sec or "أعلى" in sec: data["correct_section"] = "سعر اعلى"
        elif "اقل" in sec or "أقل" in sec:  data["correct_section"] = "سعر اقل"
        elif "موافق" in sec:                 data["correct_section"] = "موافق"
        elif "مفقود" in sec:                 data["correct_section"] = "مفقود"
        else: data["correct_section"] = expected if data.get("match") else "مفقود"
        return {"success":True, **data}
    # النص الخام بدون JSON: أي مطابقة بنسبة < 85% تُعدّ مفقودة
    _tl = txt.lower()
    match = bool(re.search(r'\btrue\b', _tl)) or "نعم" in txt
    # رفض أي مطابقة غير مؤكدة من مسار النص الخام (< 85% ثقة)
    return {
        "success": True,
        "match": False,
        "confidence": 0,
        "reason": f"[Threshold Reject] استجابة AI غير JSON — مطابقة غير مؤكدة: {txt[:200]}",
        "correct_section": "مفقود",
        "suggested_price": 0,
    }

# ══ إعادة تصنيف قسم "تحت المراجعة" ════════════════════════════════════════
_RC_BATCH = 10   # حجم الدفعة الواحدة — يمنع timeout ويحسن دقة JSON


def _fallback_review_items(batch: list, offset: int) -> list:
    """يُبقي المنتجات بحالة 'تحت المراجعة' عند فشل AI — بدلاً من إسقاطها صامتاً."""
    return [
        {"idx": offset + i, "section": "⚠️ تحت المراجعة",
         "confidence": 0, "match": False, "reason": "فشل AI"}
        for i in range(len(batch))
    ]


def _reclassify_batch(batch: list, offset: int) -> list:
    """
    تصنيف دفعة واحدة (≤ _RC_BATCH منتجات).
    offset: الرقم الأساسي للـ idx الأصلي (لتصحيح الترقيم عند دمج الدفعات).

    الجديد: يُطبّق verify_perfume_match على كل زوج قبل الإرسال للـ AI.
    المنتجات المرفوضة hard-reject تُحوَّل مباشرة لـ "مفقود" بدون استهلاك AI.
    """
    lines = []
    hard_rejected: dict[int, dict] = {}  # local_idx → نتيجة الرفض المسبق

    for i, it in enumerate(batch):
        our_name  = str(it.get("our", "") or "")
        comp_name = str(it.get("comp", "") or "")

        # ── فحص صارم مسبق (Hard Firewall) ───────────────────────────────
        _vmr = verify_perfume_match(our_name, comp_name)
        if not _vmr["ok"]:
            _logger.info(
                "_reclassify_batch HARD-REJECT [%d]: «%s» vs «%s» — %s",
                offset + i + 1, our_name[:50], comp_name[:50], _vmr["reason"],
            )
            hard_rejected[i + 1] = {
                "idx":        i + 1 + offset,
                "section":    "🔍 مفقود",
                "confidence": 0,
                "match":      False,
                "reason":     _vmr["reason"],
                "hard_reject": True,
            }
            continue  # لا يُرسل للـ AI

        diff = it.get("our_price", 0) - it.get("comp_price", 0)
        lines.append(
            f"[{i + 1}] منتجنا: {our_name} ({it.get('our_price', 0):.0f}ر.س)"
            f" vs منافس: {comp_name} ({it.get('comp_price', 0):.0f}ر.س)"
            f" | فرق: {diff:+.0f}ر.س"
        )

    # إذا كل الدفعة مرفوضة hard → لا حاجة للـ AI
    if not lines:
        return [hard_rejected[k] for k in sorted(hard_rejected)]

    prompt = (
        "حلل المنتجات التالية وأعد JSON فقط بالصيغة المطلوبة:\n"
        + "\n".join(lines)
        + "\n\nأعد JSON فقط — لا أي نص قبله أو بعده:\n"
        + '{"results":[{"idx":1,"section":"القسم","confidence":85,"match":true,"reason":"سبب"},...]} '
        + f"(يجب أن يحتوي على {len(lines)} عنصر بالضبط)"
    )
    sys_prompt = PAGE_PROMPTS["reclassify"]
    txt = _call_gemini(prompt, sys_prompt, temperature=0.1) or _call_openrouter(prompt, sys_prompt)
    if not txt:
        _logger.warning("_reclassify_batch: AI لم يُرجع نصاً — %d منتج يبقى تحت المراجعة", len(lines))
        ai_results = _fallback_review_items(batch, offset)
    else:
        data = _parse_json(txt)
        if not (data and "results" in data):
            _logger.warning("_reclassify_batch: JSON غير صالح — %d منتج يبقى تحت المراجعة | نص[:200]=%s",
                            len(lines), (txt or "")[:200])
            ai_results = _fallback_review_items(batch, offset)
        else:
            ai_results = []
            for r in data["results"]:
                try:
                    local_idx = int(r.get("idx", 0) or 0)
                except Exception:
                    local_idx = 0
                r["idx"] = local_idx + offset

                # ── فحص بعد AI: رفض أي مطابقة confidence < 85 ──────────
                if r.get("match") and int(r.get("confidence", 0) or 0) < 85:
                    r["match"]     = False
                    r["section"]   = "مفقود"
                    r["reason"]    = (
                        f"[Threshold Reject] confidence={r.get('confidence')}% < 85% — {r.get('reason','')}"
                    )

                sec = r.get("section", "")
                if "اعلى" in sec or "أعلى" in sec:
                    r["section"] = "🔴 سعر أعلى"
                elif "اقل" in sec or "أقل" in sec:
                    r["section"] = "🟢 سعر أقل"
                elif "موافق" in sec:
                    r["section"] = "✅ موافق"
                elif "مفقود" in sec:
                    r["section"] = "🔍 مفقود"
                else:
                    r["section"] = "⚠️ تحت المراجعة"
                ai_results.append(r)

    # ── دمج نتائج AI مع المرفوضات المسبقة وترتيبها ───────────────────────
    out = list(hard_rejected.values()) + ai_results
    out.sort(key=lambda x: x.get("idx", 0))
    return out


def reclassify_review_items(items: list) -> list:
    """
    يُعيد تصنيف قائمة المنتجات (تحت المراجعة) عبر Gemini Flash.
    يُعالج في دفعات صغيرة (_RC_BATCH) لتجنب timeout وضمان JSON سليم.
    يُعيد قائمة نتائج مدمجة من كل الدفعات.
    """
    if not items:
        return []
    all_results: list = []
    for start in range(0, len(items), _RC_BATCH):
        batch = items[start: start + _RC_BATCH]
        batch_res = _reclassify_batch(batch, offset=start)
        all_results.extend(batch_res)
    return all_results

# ══ بحث أسعار السوق ══════════════════════════════════════════════════════
def search_market_price(product_name, our_price=0):
    # البحث في أشهر المتاجر السعودية (سلة، زد، نايس ون، قولدن سنت، خبير العطور)
    queries = [
        f"سعر {product_name} السعودية نايس ون قولدن سنت سلة",
        f"سعر {product_name} في المتاجر السعودية 2026",
        f"مقارنة أسعار {product_name} السعودية",
        f"{product_name} price Saudi Arabia perfume shop",
    ]
    all_results = []
    for q in queries[:3]:  # استخدام أول 3 استعلامات
        ddg = _search_ddg(q)
        if ddg: all_results.extend(ddg[:3])
    
    web_ctx = "\n".join(f"- {r['title']}: {r['snippet'][:120]}" for r in all_results) if all_results else ""
    
    prompt = f"""تحليل سوق دقيق للمنتج في السعودية (مارس 2026):
المنتج: {product_name}
سعرنا الحالي: {our_price:.0f} ريال

المعلومات المستخرجة من الويب:
{web_ctx}

المطلوب تحليل JSON مفصل:
1. متوسط السعر في السوق السعودي.
2. أرخص سعر متاح حالياً واسم المتجر.
3. قائمة المنافسين المباشرين وأسعارهم (نايس ون، قولدن سنت، لودوريه، بيوتي ستور، إلخ).
4. حالة التوفر (متوفر/غير متوفر).
5. توصية تسعير ذكية لمتجر مهووس ليكون الأكثر تنافسية.
6. نسبة الثقة في البيانات (0-100)."""
    sys = PAGE_PROMPTS["market_search"]
    txt = _call_gemini(prompt, sys, grounding=True)
    if not txt: txt = _call_gemini(prompt, sys)
    if not txt: txt = _call_openrouter(prompt, sys)
    if not txt: return {"success":False,"market_price":0}
    data = _parse_json(txt)
    if data:
        data["web_context"] = web_ctx
        return {"success":True, **data}
    return {"success":True,"market_price":our_price,"recommendation":txt[:400],"web_context":web_ctx}

# ══ تحليل عميق ══════════════════════════════════════════════════════════════
def ai_deep_analysis(our_product, our_price, comp_product, comp_price, section="general", brand=""):
    diff = our_price - comp_price if our_price > 0 and comp_price > 0 else 0
    diff_pct = (abs(diff)/comp_price*100) if comp_price > 0 else 0
    ddg = _search_ddg(f"سعر {our_product} السعودية")
    web_ctx = "\n".join(f"- {r['snippet'][:80]}" for r in ddg[:2]) if ddg else ""
    guidance = {
        "🔴 سعر أعلى": f"سعرنا اعلى بـ{diff:.0f}ريال ({diff_pct:.1f}%). هل يجب خفضه؟",
        "🟢 سعر أقل":  f"سعرنا اقل بـ{abs(diff):.0f}ريال ({diff_pct:.1f}%). كم يمكن رفعه؟",
        "✅ موافق":     "السعر تنافسي. هل نحافظ عليه؟",
        "⚠️ تحت المراجعة": "المطابقة غير مؤكدة. هل هما نفس المنتج؟",
    }.get(section, "")
    prompt = f"""تحليل تسعير عميق:
منتجنا: {our_product} | سعرنا: {our_price:.0f} ريال
المنافس: {comp_product} | سعره: {comp_price:.0f} ريال
الفرق: {diff:+.0f} ريال | {diff_pct:.1f}% | {guidance}
{f"معلومات السوق:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب بتقرير مختصر: هل المطابقة صحيحة؟ السعر المقترح بالرقم؟ الاجراء الفوري؟"""
    txt = _call_gemini(prompt, grounding=bool(web_ctx)) or _call_openrouter(prompt)
    if txt: return {"success":True,"response":txt,"source":"Gemini" + (" + ويب" if web_ctx else "")}
    return {"success":False,"response":"فشل التحليل"}

# ══ بحث mahwous.com ══════════════════════════════════════════════════════════
def search_mahwous(product_name):
    ddg = _search_ddg(f"site:mahwous.com {product_name}")
    web_ctx = "\n".join(r["snippet"][:100] for r in ddg[:2]) if ddg else ""
    prompt = f"""هل العطر {product_name} متوفر في متجر مهووس؟
{f"نتائج:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب JSON: {{"likely_available":true/false,"confidence":0-100,"similar_products":[],
"add_recommendation":"عالية/متوسطة/منخفضة","reason":"","suggested_price":0}}"""
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt: return {"success":False}
    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":True,"likely_available":False,"confidence":50,"reason":txt[:150]}

# ══ تحقق مكرر ════════════════════════════════════════════════════════════════
def check_duplicate(product_name, our_products):
    if not our_products: return {"success":True,"response":"لا توجد بيانات"}
    prompt = f"""هل العطر {product_name} موجود بشكل مشابه في هذه القائمة؟
القائمة: {', '.join(str(p) for p in our_products[:30])}
اجب: نعم (وذكر اقرب مطابقة) او لا مع السبب."""
    return call_ai(prompt, "missing")

# ══ تحليل مجمع ════════════════════════════════════════════════════════════════
def bulk_verify(items, section="general"):
    if not items: return {"success":False,"response":"لا توجد منتجات"}
    lines = "\n".join(
        f"{i+1}. {it.get('our','')} vs {it.get('comp','')} | "
        f"سعرنا: {it.get('our_price',0):.0f} | منافس: {it.get('comp_price',0):.0f} | "
        f"فرق: {it.get('our_price',0)-it.get('comp_price',0):+.0f}"
        for i,it in enumerate(items))
    instructions = {
        "price_raise": "سعرنا اعلى. لكل منتج: هل المطابقة صحيحة؟ هل نخفض؟ السعر المقترح.",
        "price_lower": "سعرنا اقل = ربح ضائع. لكل منتج: هل يمكن رفعه؟ السعر الامثل.",
        "review": "مطابقات غير مؤكدة. لكل منتج: هل هما نفس العطر فعلا؟ نعم/لا/غير متاكد.",
        "approved": "منتجات موافق عليها. راجعها وتاكد انها لا تزال تنافسية.",
    }
    prompt = f"{instructions.get(section,'حلل واعط توصية.')}\n\nالمنتجات:\n{lines}"
    return call_ai(prompt, section)

# ══ معالجة النص الملصوق ═══════════════════════════════════════════════════
def analyze_paste(text, context=""):
    prompt = f"""المستخدم لصق هذا النص:
---
{text[:5000]}
---
حلل واستخرج: قائمة منتجات؟ اسعار؟ اوامر؟ اعط توصيات مفيدة. اجب بالعربية منظم."""
    return call_ai(prompt, "general")

# ══ دوال متوافقة مع app.py ════════════════════════════════════════════════
def chat_with_ai(msg, history=None, ctx=""): return gemini_chat(msg, history, ctx)
def analyze_product(p, price=0): return call_ai(f"حلل: {p} ({price:.0f}ريال)", "general")
def suggest_price(p, comp_price): return call_ai(f"اقترح سعرا لـ {p} بدلا من {comp_price:.0f}ريال", "general")
def process_paste(text): return analyze_paste(text)


# ══════════════════════════════════════════════════════════════════════════
#  محرك إثراء المحتوى التسويقي (Content Enrichment Engine)
#  يولّد وصفاً Markdown مع ربط الماركة والقسم من ملفات المتجر الفعلية
# ══════════════════════════════════════════════════════════════════════════
import pandas as _pd
import os as _os
import functools as _functools

from engines.prompts import (
    SEO_CONTENT_PROMPT,
    SALLA_BRANDS_FILE, SALLA_BRANDS_COL,
    SALLA_CATEGORIES_FILE, SALLA_CATEGORIES_COL,
    BRANDS_CSV_FILE, BRANDS_CSV_COL,
    CATEGORIES_CSV_FILE, CATEGORIES_CSV_COL,
)


def _load_catalog_by_colname(csv_path: str, col_name: str) -> list[str]:
    """
    يقرأ عمود CSV باسمه الصريح (للملفات الرسمية من سلة).
    يدعم الترميزات العربية الشائعة.
    """
    for enc in ("utf-8-sig", "utf-8", "cp1256"):
        try:
            df = _pd.read_csv(csv_path, header=0, encoding=enc)
            if col_name in df.columns:
                return [str(v).strip() for v in df[col_name].dropna().tolist()
                        if str(v).strip() and str(v) not in ("nan", "None")]
        except Exception:
            continue
    return []


def _load_catalog_list(csv_path: str, col_idx: int) -> list[str]:
    """
    يقرأ عمود CSV برقمه (للملفات الاحتياطية العامة).
    يدعم الترميزات العربية الشائعة (UTF-8 / cp1256).
    """
    for enc in ("utf-8-sig", "utf-8", "cp1256"):
        try:
            df = _pd.read_csv(csv_path, header=0, encoding=enc)
            col = df.iloc[:, col_idx]
            return [str(v).strip() for v in col.dropna().tolist()
                    if str(v).strip() and str(v) not in ("nan", "None")]
        except Exception:
            continue
    return []


def _find_catalog_file(salla_filename: str, fallback_filename: str) -> tuple[str, bool]:
    """
    يحدّد مسار ملف الكتالوج بالأولوية التالية:
    1. ملف سلة الرسمي في DATA_DIR (Railway Volume)
    2. ملف سلة الرسمي في جذر المشروع (للتطوير المحلي)
    3. الملف الاحتياطي عبر get_catalog_data_path

    يُعيد (المسار, هل_هو_ملف_سلة)
    """
    import os as _os_local
    from utils.data_paths import get_catalog_data_path

    # 1. ملف سلة في DATA_DIR
    data_dir = (_os_local.environ.get("DATA_DIR") or "").strip()
    if data_dir:
        salla_path = _os_local.path.join(data_dir, salla_filename)
        if _os_local.path.exists(salla_path):
            return salla_path, True

    # 2. ملف سلة في جذر المشروع (بجانب app.py)
    root = _os_local.path.dirname(_os_local.path.dirname(_os_local.path.abspath(__file__)))
    salla_root_path = _os_local.path.join(root, salla_filename)
    if _os_local.path.exists(salla_root_path):
        return salla_root_path, True

    # 3. الملف الاحتياطي
    return get_catalog_data_path(fallback_filename), False


def _resolve_catalog_paths() -> tuple[str, str]:
    """يحدد مسار brands.csv و categories.csv عبر data_paths (ملفات احتياطية)."""
    from utils.data_paths import get_catalog_data_path
    return (
        get_catalog_data_path(BRANDS_CSV_FILE),
        get_catalog_data_path(CATEGORIES_CSV_FILE),
    )


def _build_brands_list() -> str:
    """
    يبني قائمة الماركات بأولوية: ملف سلة الرسمي → الملف الاحتياطي.
    النتيجة مُخزَّنة في ذاكرة العملية (LRU cache) — لا يُعيد قراءة الملف مع كل طلب.
    استدعِ `_build_brands_list.cache_clear()` إذا أردت إعادة القراءة (مثلاً بعد رفع ملف جديد).
    """
    path, is_salla = _find_catalog_file(SALLA_BRANDS_FILE, BRANDS_CSV_FILE)
    if is_salla:
        items = _load_catalog_by_colname(path, SALLA_BRANDS_COL)
    else:
        items = _load_catalog_list(path, BRANDS_CSV_COL)
    if items:
        return "\n".join(f"- {b}" for b in items)
    return "⚠️ لم يُعثر على ملف الماركات — يرجى رفع «ماركات مهووس.csv» في مجلد /data"


# ── ذاكرة تخزين مؤقت (TTL 6 ساعات) تحمي الخادم من إعادة قراءة الملفات ──
@_functools.lru_cache(maxsize=1)
def _brands_list_cached() -> str:
    return _build_brands_list()


def _build_categories_list() -> str:
    """
    يبني قائمة الأقسام بأولوية: ملف سلة الرسمي → الملف الاحتياطي.
    """
    path, is_salla = _find_catalog_file(SALLA_CATEGORIES_FILE, CATEGORIES_CSV_FILE)
    if is_salla:
        items = _load_catalog_by_colname(path, SALLA_CATEGORIES_COL)
    else:
        items = _load_catalog_list(path, CATEGORIES_CSV_COL)
    if items:
        return "\n".join(f"- {c}" for c in items)
    return "⚠️ لم يُعثر على ملف الأقسام — يرجى رفع «تصنيفات مهووس.csv» في مجلد /data"


@_functools.lru_cache(maxsize=1)
def _categories_list_cached() -> str:
    return _build_categories_list()


def clear_catalog_cache() -> None:
    """
    يمسح ذاكرة التخزين المؤقت لقوائم الماركات والأقسام.
    استدعِها بعد رفع ملفات جديدة من الواجهة.
    """
    _brands_list_cached.cache_clear()
    _categories_list_cached.cache_clear()


def generate_seo_description(raw_product_data: str) -> dict:
    """
    توليد الوصف التسويقي SEO بتنسيق Markdown مع ربط:
    - exact_brand    : الماركة المطابقة تماماً من brands.csv
    - exact_category : القسم المطابق تماماً من categories.csv
    - markdown_desc  : الوصف الجاهز

    يستخدم Gemini → OpenRouter → Cohere بالتتابع
    (نفس منطق call_ai في هذا الملف).

    المعاملات:
      raw_product_data : نص خام يصف المنتج (اسم، سعر، URL، إلخ)

    يُعيد dict:
      {"exact_brand": str, "exact_category": str, "markdown_desc": str}
      أو {"error": str} عند الفشل الكامل
    """
    if not raw_product_data or not raw_product_data.strip():
        return {"error": "raw_product_data فارغ — لا شيء لتوليده"}

    prompt = SEO_CONTENT_PROMPT.format(
        brands_list=_brands_list_cached(),
        categories_list=_categories_list_cached(),
        raw_product_data=raw_product_data.strip()[:4000],  # حد آمن
    )

    # حرارة 0.1 (شبه حتمي) لضمان النسخ الحرفي من قوائم سلة
    raw_text = _call_gemini(prompt, temperature=0.1, max_tokens=2048)
    if not raw_text:
        raw_text = _call_openrouter(prompt)
    if not raw_text:
        raw_text = _call_cohere(prompt)

    if not raw_text:
        _log_err("generate_seo_description", "جميع مزودي AI فشلوا")
        return {"error": "فشلت جميع محاولات الاتصال بالذكاء الاصطناعي"}

    data = _parse_json(raw_text)
    if not data:
        # إن أخفق JSON نعيد الـ markdown كاملاً بدون ربط
        _log_err("generate_seo_description", f"فشل تحليل JSON — سنعيد النص خاماً: {raw_text[:120]}")
        return {
            "exact_brand": "",
            "exact_category": "",
            "suggested_new_brand": "",
            "markdown_desc": raw_text.strip(),
            "warning": "JSON parse failed — returned raw text",
        }

    # ── التقاط الماركات المفقودة (Auto-Capture Missing Brands) ─────────────
    suggested_brand = str(data.get("suggested_new_brand", "") or "").strip()
    if suggested_brand:
        from utils.data_paths import get_catalog_data_path
        _missing_file = get_catalog_data_path("missing_brands.txt")
        try:
            # قراءة الماركات المسجلة مسبقاً لمنع التكرار
            _existing: set[str] = set()
            if _os.path.exists(_missing_file):
                with open(_missing_file, "r", encoding="utf-8") as _fh:
                    _existing = {ln.strip() for ln in _fh if ln.strip()}
            # تسجيل الماركة فقط إذا لم تكن مسجلة من قبل
            if suggested_brand not in _existing:
                _os.makedirs(_os.path.dirname(_missing_file), exist_ok=True)
                with open(_missing_file, "a", encoding="utf-8") as _fh:
                    _fh.write(f"{suggested_brand}\n")
        except Exception as _capture_err:
            _log_err("generate_seo_description", f"فشل حفظ الماركة المقترحة: {_capture_err}")
    # ────────────────────────────────────────────────────────────────────────

    return {
        "exact_brand":         str(data.get("exact_brand", "") or "").strip(),
        "exact_category":      str(data.get("exact_category", "") or "").strip(),
        "suggested_new_brand": suggested_brand,
        "markdown_desc":       str(data.get("markdown_desc", "") or "").strip(),
    }


def get_catalog_status() -> dict:
    """
    يعيد حالة ملفات الكتالوج (للعرض في واجهة الإعدادات):
    - ملفات سلة الرسمية (إن وُجدت)
    - الملفات الاحتياطية
    - missing_brands.txt
    """
    from utils.data_paths import get_catalog_data_path

    def _stat_salla(salla_file: str, salla_col: str, fallback_file: str, fallback_col_idx: int) -> dict:
        path, is_salla = _find_catalog_file(salla_file, fallback_file)
        source = "سلة (رسمي)" if is_salla else "احتياطي (generic)"
        if not _os.path.exists(path):
            return {"found": False, "path": path, "source": source, "count": 0, "sample": []}
        if is_salla:
            items = _load_catalog_by_colname(path, salla_col)
        else:
            items = _load_catalog_list(path, fallback_col_idx)
        return {"found": True, "path": path, "source": source,
                "count": len(items), "sample": items[:5]}

    # حالة missing_brands.txt
    missing_path = get_catalog_data_path("missing_brands.txt")
    if _os.path.exists(missing_path):
        try:
            with open(missing_path, "r", encoding="utf-8") as _fh:
                _mb = [ln.strip() for ln in _fh if ln.strip()]
            missing_stat = {"found": True, "path": missing_path,
                            "count": len(_mb), "sample": _mb[:10]}
        except Exception:
            missing_stat = {"found": True, "path": missing_path, "count": -1}
    else:
        missing_stat = {"found": False, "path": missing_path, "count": 0}

    return {
        "brands":         _stat_salla(SALLA_BRANDS_FILE, SALLA_BRANDS_COL,
                                      BRANDS_CSV_FILE, BRANDS_CSV_COL),
        "categories":     _stat_salla(SALLA_CATEGORIES_FILE, SALLA_CATEGORIES_COL,
                                      CATEGORIES_CSV_FILE, CATEGORIES_CSV_COL),
        "missing_brands": missing_stat,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  مولّد الوصف الآلي لسلة — AI Product Description Generator
# ══════════════════════════════════════════════════════════════════════════════

def generate_salla_html_description(product_name: str, raw_scraped_text: str = "") -> str:
    """
    يستخدم Gemini لاستخراج المكونات الحقيقية من النص الخام وصياغتها بتنسيق HTML
    الخاص بمتجر "مهووس" للعطور — مناسب للرفع المباشر على سلة.

    القواعد:
    - درجة الحرارة 0.1 — شبه حتمي لمنع التأليف في المكونات.
    - يمنع منعاً باتاً تأليف مكونات غير موجودة في النص الخام.
    - المخرج HTML نظيف بدون markdown أو code fences.
    """
    raw_section = (
        f"\nالمعلومات المسحوبة من صفحة المنافس:\n{raw_scraped_text[:1200]}"
        if (raw_scraped_text or "").strip()
        else "\nلم تتوفر معلومات مسحوبة — اعتمد على اسم العطر فقط."
    )

    prompt = f"""أنت خبير عطور وكاتب محتوى محترف لمتجر "مهووس" للعطور.
اسم العطر: {product_name}{raw_section}

المطلوب:
1. استخرج المكونات العطرية الحقيقية (القمة، القلب، القاعدة) من المعلومات المسحوبة. يمنع منعاً باتاً تأليف أي مكونات غير موجودة في المصدر. إذا لم تجد مكونات محددة اكتب فقط: مزيج عطري فاخر وسري.
2. اكتب وصفاً تسويقياً جذاباً من سطرين يعكس شخصية العطر.
3. أعد المخرجات بصيغة HTML نظيفة تماماً — بدون markdown وبدون كود مُغلَّق (code fences).

هيكل HTML المطلوب حرفياً (لا تغيّر الوسوم):
<h2>وصف العطر</h2>
<p>[وصف تسويقي جذاب من سطرين]</p>
<h3>الهرم العطري</h3>
<ul>
<li><strong>إفتتاحية العطر:</strong> [المكونات]</li>
<li><strong>قلب العطر:</strong> [المكونات]</li>
<li><strong>قاعدة العطر:</strong> [المكونات]</li>
</ul>
<h3>لمسة خبير من مهووس</h3>
<p>الفوحان: [/10] | الثبات: [/10] | نصيحة: [نصيحة قصيرة للاستخدام]</p>"""

    raw = _call_gemini(prompt, temperature=0.1, max_tokens=2048)

    if not raw:
        # fallback: OpenRouter ثم Cohere
        raw = _call_openrouter(prompt) or _call_cohere(prompt)

    if not raw:
        return (
            f"<h2>وصف العطر</h2>"
            f"<p>{product_name} — عطر فاخر يجمع بين الأناقة والعراقة.</p>"
            f"<h3>الهرم العطري</h3>"
            f"<ul><li><strong>إفتتاحية العطر:</strong> مزيج عطري فاخر وسري</li>"
            f"<li><strong>قلب العطر:</strong> مزيج عطري فاخر وسري</li>"
            f"<li><strong>قاعدة العطر:</strong> مزيج عطري فاخر وسري</li></ul>"
            f"<h3>لمسة خبير من مهووس</h3>"
            f"<p>الفوحان: —/10 | الثبات: —/10 | نصيحة: ارتدِه في المناسبات الخاصة.</p>"
        )

    # تنظيف: إزالة markdown code fences إن وُجدت
    cleaned = re.sub(r"```(?:html)?\s*", "", raw, flags=re.I)
    cleaned = re.sub(r"```", "", cleaned).strip()
    return cleaned


def _parse_brand_json_block(text: str) -> dict:
    """يستخرج كائن JSON من مخرجات Gemini (مع أو بدون code fences)."""
    if not text or not str(text).strip():
        return {}
    t = str(text).strip()
    m = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", t, re.I)
    if m:
        try:
            j = json.loads(m.group(1).strip())
            if isinstance(j, dict) and "brand_name" in j:
                return j
        except Exception:
            pass
    last = t.rfind("\n{")
    if last == -1:
        last = t.rfind("{")
    if last != -1:
        tail = t[last:]
        depth = 0
        end = None
        for i, c in enumerate(tail):
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end:
            try:
                j = json.loads(tail[:end])
                if isinstance(j, dict):
                    return j
            except Exception:
                pass
    return {}


def _clamp_salla_brand_dict(d: dict, original_brand: str) -> dict:
    """فرض الحدود القصوى لسلة بعد الاستلام من النموذج."""
    bn = str(d.get("brand_name", "") or "").strip()
    if not bn and original_brand:
        bn = str(original_brand).strip()[:30]
    if len(bn) > 30:
        bn = bn[:30]
    desc = str(d.get("description", "") or "").strip()
    if len(desc) > 250:
        desc = desc[:250]
    stitle = str(d.get("seo_title", "") or "").strip()
    if len(stitle) > 70:
        stitle = stitle[:70]
    sdesc = str(d.get("seo_desc", "") or "").strip()
    if len(sdesc) > 155:
        sdesc = sdesc[:155]
    surl = str(d.get("seo_url", "") or "").strip().lower()
    surl = re.sub(r"\s+", "_", surl)
    surl = re.sub(r"[^a-z0-9_-]", "", surl)
    if not surl or len(surl) < 3:
        safe = re.sub(r"[^a-z0-9]+", "_", original_brand.lower().strip())[:24].strip("_")
        if not safe:
            safe = hashlib.md5(original_brand.encode("utf-8")).hexdigest()[:10]
        surl = f"{safe}_mahwous"
    elif "mahwous" not in surl:
        surl = (surl + "_mahwous")[:80]
    surl = surl[:80]
    return {
        "brand_name": bn,
        "description": desc,
        "seo_title": stitle,
        "seo_url": surl,
        "seo_desc": sdesc,
    }


def generate_salla_brand_info(brand_name: str) -> dict:
    """
    يولد بيانات الماركة الكاملة بتنسيق متوافق مع أعمدة سلة الرسمية.

    الأعمدة المستهدفة (من Brands_Salla.csv):
      - brand_name   : اسم الماركة (max 30 حرف)
      - description  : وصف مختصر (max 255 حرف)
      - logo_url     : رابط الشعار (فارغ — سلة لا تقبل رفع روابط خارجية مباشرة)
      - seo_title    : عنوان صفحة العلامة (max 70 حرف)
      - seo_url      : رابط SEO (lowercase_snake + _mahwous)
      - seo_desc     : وصف الصفحة للبحث (max 155 حرف)
      - logo_prompt  : برومت AI لتوليد لوجو الماركة
    """
    bn = str(brand_name or "").strip()
    if not bn or bn.lower() in ("nan", "none"):
        return _clamp_salla_brand_dict({}, "")

    # ── Prompt محسّن مع Few-Shot من brands.csv الحقيقي ───────────────────
    prompt = f"""أنت خبير عطور وSEO عالمي تعمل لمتجر "مهووس" للعطور الفاخرة في السعودية.
اكتشفنا ماركة عطور جديدة غير مسجّلة اسمها: "{bn}".

نماذج جودة من ملف ماركاتنا (احتذِ بها في الطول والأسلوب):
- جيفنشي | Givenchy → "جيفنشي هي علامة فرنسية فاخرة متخصصة في الأزياء والعطور ومستحضرات التجميل. تأسست عام 1952 بواسطة هوبير دو جيفنشي، تتميز بتصاميمها العصرية والفاخرة."
- فيرساتشي | Versace → "دار فيرساتشي الإيطالية للرفاهية تأسست عام 1978، تقدم تصاميمًا جريئة في الأزياء والإكسسوارات والعطور والديكور."
- أتلييه دي أور Atelier Des Ors → "دار عطور فرنسية فاخرة تأسست عام 2015، متخصصة في عطور نادرة مزينة بذهب حقيقي، تجمع الأصالة والحداثة بتوقيع باريسي."

الحدود الصارمة (عدّ كل حرف بما فيه المسافات والرموز):
1. "brand_name": الاسم باللغتين (العربي | English) — حد أقصى 30 حرفاً.
2. "description": وصف إبداعي يجسّد روح الماركة وفخامتها — حد أقصى 255 حرفاً.
3. "seo_title": عطور [اسم] + جملة تسويقية + من متجر مهووس — حد أقصى 70 حرفاً.
4. "seo_url": اسم الماركة بالإنجليزية (lowercase_snake) + "_mahwous" — مثال: dior_mahwous.
5. "seo_desc": ملخص احترافي لجذب جوجل — حد أقصى 155 حرفاً.
6. "logo_prompt": برومت إنجليزي لجلب لوجو الماركة بـ AI (جملة وصفية احترافية).

⚠️ أعد JSON فقط — بدون markdown، بدون أي نص قبله أو بعده.
المفاتيح حرفياً: brand_name, description, seo_title, seo_url, seo_desc, logo_prompt"""

    try:
        raw = _call_gemini(prompt, temperature=0.1, max_tokens=1024)
        if not raw:
            raw = _call_openrouter(prompt) or _call_cohere(prompt)
        parsed = _parse_brand_json_block(raw) if raw else {}
        if parsed:
            result = _clamp_salla_brand_dict(parsed, bn)
            result["logo_url"]    = str(parsed.get("logo_url",    "") or "").strip()
            result["logo_prompt"] = str(parsed.get("logo_prompt", "") or "").strip()
            return result
    except Exception as _e:
        _log_err("generate_salla_brand_info", str(_e)[:80])

    # ── Fallback — يلتزم بالحدود ─────────────────────────────────────────
    safe_name = bn[:14]
    en_part   = safe_name
    if "|" in bn:
        parts   = [p.strip() for p in bn.split("|")]
        en_part = next((p for p in parts if re.search(r"[a-zA-Z]", p)), parts[-1])
    slug = re.sub(r"[^a-z0-9]+", "_", en_part.lower().strip())[:18].strip("_") or "brand"
    result = _clamp_salla_brand_dict(
        {
            "brand_name":  (f"{safe_name} | {en_part}")[:30],
            "description": (
                f"عطور {safe_name} الفاخرة الأصيلة — اكتشف التميز والجاذبية مع تشكيلتنا "
                f"المختارة بعناية في متجر مهووس للعطور."
            )[:255],
            "seo_title":   (f"عطور {safe_name} الأصلية | متجر مهووس")[:70],
            "seo_url":     f"{slug}_mahwous",
            "seo_desc":    (
                f"تسوق عطور {safe_name} الأصلية بأسعار تنافسية من متجر مهووس. "
                f"ضمان الأصالة وتوصيل سريع لجميع مناطق المملكة."
            )[:155],
        },
        bn,
    )
    result["logo_url"]    = ""
    result["logo_prompt"] = (
        f"Professional luxury perfume brand logo for {en_part}, "
        f"minimalist gold and black design, white background, high resolution."
    )
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  🦅 Hawk-Eye Vision — التحقق البصري عبر Gemini Vision
# ══════════════════════════════════════════════════════════════════════════════

def get_base64_from_url(url: str, timeout: int = 10) -> str | None:
    """
    يحمّل صورة من URL ويحوّلها إلى Base64.

    Returns:
        سلسلة Base64 عند النجاح، أو None عند أي فشل (الشبكة، MIME، إلخ).
    """
    if not (url or "").strip():
        return None
    try:
        resp = requests.get(
            url.strip(),
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 (compatible; MahwousVision/1.0)"},
        )
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        # رفض الردود غير الصورية (HTML، JSON، إلخ)
        if not any(t in content_type for t in ("image/", "application/octet-stream")):
            return None
        return base64.b64encode(resp.content).decode("utf-8")
    except Exception:
        return None


def visual_verify_match(
    our_img_url: str,
    comp_img_url: str,
    product_name: str,
) -> dict:
    """
    يقارن بصرياً بين صورتين عبر Gemini Vision للتأكد من تطابق العطر الفعلي.

    يفيد في حلّ حالات "المنطقة الرمادية" حيث الأسماء متشابهة لكن قد تكون
    إصدارات مختلفة (مثال: نفس الماركة بحجم زجاجة مختلف أو لون مختلف).

    Args:
        our_img_url:  رابط صورة منتجنا.
        comp_img_url: رابط صورة منتج المنافس.
        product_name: اسم العطر المفترض (للسياق في الـ Prompt).

    Returns:
        dict مع المفاتيح:
            - match  (bool):   هل الصورتان لنفس العطر تماماً؟
            - reason (str):    تبرير مرئي موجز (سطر واحد).
            - source (str):    مصدر القرار ("gemini_vision" | "fallback").
    """
    # ── التحقق من توفر الصور ────────────────────────────────────────────
    if not our_img_url or not comp_img_url:
        return {"match": False, "reason": "الصور غير متوفرة للتحقق البصري.", "source": "fallback"}

    our_b64  = get_base64_from_url(our_img_url)
    comp_b64 = get_base64_from_url(comp_img_url)

    if not our_b64:
        return {"match": False, "reason": "فشل تحميل صورة منتجنا.", "source": "fallback"}
    if not comp_b64:
        return {"match": False, "reason": "فشل تحميل صورة المنافس.", "source": "fallback"}

    # ── بناء الـ Prompt ──────────────────────────────────────────────────
    prompt = (
        f'أنت خبير في التعرف البصري على زجاجات العطور الفاخرة.\n'
        f'أمامك صورتان لعطر يُفترض أنه: "{product_name}".\n'
        f'دقّق جيداً في:\n'
        f'  1. شكل ولون الزجاجة والسائل داخلها.\n'
        f'  2. شكل الغطاء ومادته.\n'
        f'  3. الملصق (Label) وخط الكتابة عليه.\n'
        f'هل الصورتان لنفس العطر تماماً (نفس الإصدار والتركيز والحجم)?\n'
        f'أرجع JSON فقط — لا أي نص قبله أو بعده:\n'
        f'{{"is_identical": true/false, "visual_reason": "تبرير دقيق من سطر واحد"}}'
    )

    payload = {
        "contents": [{
            "parts": [
                {"text": prompt},
                {"inline_data": {"mime_type": "image/jpeg", "data": our_b64}},
                {"inline_data": {"mime_type": "image/jpeg", "data": comp_b64}},
            ]
        }],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 200},
    }

    # ── استدعاء Gemini Vision (يجرّب كل مفتاح متاح) ────────────────────
    # gemini-2.0-flash يدعم الرؤية + أسرع وأرخص من 1.5-flash
    vision_url_tpl = (
        "https://generativelanguage.googleapis.com/v1beta"
        f"/models/{_GM}:generateContent?key={{key}}"
    )
    for key in (GEMINI_API_KEYS or []):
        if not key:
            continue
        try:
            r = requests.post(
                vision_url_tpl.format(key=key),
                json=payload,
                timeout=30,
            )
            if r.status_code != 200:
                continue
            raw_txt = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            clean   = re.sub(r"```json|```", "", raw_txt).strip()
            s = clean.find("{"); e = clean.rfind("}") + 1
            if s < 0 or e <= s:
                continue
            data = json.loads(clean[s:e])
            return {
                "match":  bool(data.get("is_identical", False)),
                "reason": str(data.get("visual_reason", "")).strip(),
                "source": "gemini_vision",
            }
        except Exception:
            continue

    # ── Graceful fallback — لا ينهار التطبيق أبداً ────────────────────
    return {
        "match":  False,
        "reason": "تعذّر الاتصال بـ Gemini Vision — يُرجى إعادة المحاولة لاحقاً.",
        "source": "fallback",
    }
