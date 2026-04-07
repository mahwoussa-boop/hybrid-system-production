"""
config.py - الإعدادات المركزية v19.0
المفاتيح: أولاً os.environ (Railway / Docker)، ثم Streamlit Secrets عند التوفر.
"""
import json as _json
import os as _os

from utils.data_paths import get_data_db_path

# ===== معلومات التطبيق =====
APP_TITLE   = "نظام التسعير الذكي - مهووس"
APP_NAME    = APP_TITLE
APP_VERSION = "v26.0"
APP_ICON    = "🧪"
GEMINI_MODEL = "gemini-2.0-flash"   # النموذج المستقر الموصى به

# ══════════════════════════════════════════════
#  قراءة Secrets بطريقة آمنة 100%
#  تدعم 3 أساليب Streamlit
# ══════════════════════════════════════════════
def _s(key, default=""):
    """
    يقرأ الإعداد بـ 3 طرق (بالترتيب):
    1. os.environ              Railway / Docker / متغيرات النظام
    2. st.secrets[key]         Streamlit Cloud عند وجود secrets.toml
    3. default
    """
    # 1. البيئة أولاً — مطلوب على Railway حيث لا يوجد secrets.toml
    v = _os.environ.get(key, "")
    if v:
        return v
    # 2. st.secrets (Streamlit Cloud فقط - يُستدعى عند التشغيل)
    try:
        import streamlit as st
        v = st.secrets[key]
        if v is not None:
            return str(v) if not isinstance(v, (list, dict)) else v
    except Exception:
        pass
    return default


def _parse_gemini_keys():
    """
    يجمع مفاتيح Gemini من أي صيغة:
    • GEMINI_API_KEYS = '["key1","key2","key3"]'  (JSON string)
    • GEMINI_API_KEYS = ["key1","key2"]            (TOML array)
    • GEMINI_API_KEY  = "key1"                     (مفتاح واحد)
    • GEMINI_KEY_1 / GEMINI_KEY_2 / ...           (مفاتيح منفصلة)
    """
    keys = []

    # ─── المحاولة 1: GEMINI_API_KEYS (JSON string أو TOML array) ───
    raw = _s("GEMINI_API_KEYS", "")

    if isinstance(raw, list):
        # TOML array مباشرة
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        # قد تكون JSON string
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                # ربما string بدون quotes صحيحة → نظفها
                clean = raw.strip("[]").replace('"','').replace("'",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]

    # ─── المحاولة 2: GEMINI_API_KEY (مفتاح واحد) ───
    single = _s("GEMINI_API_KEY", "")
    if single and single not in keys:
        keys.append(single)

    # ─── المحاولة 3: مفاتيح منفصلة ───
    for n in ["GEMINI_KEY_1","GEMINI_KEY_2","GEMINI_KEY_3",
              "GEMINI_KEY_4","GEMINI_KEY_5"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # تنظيف نهائي: إزالة المفاتيح الفارغة أو القصيرة
    keys = [k.strip() for k in keys if k and len(k) > 20]
    return keys


# ══════════════════════════════════════════════
#  المفاتيح الفعلية
# ══════════════════════════════════════════════
GEMINI_API_KEYS    = _parse_gemini_keys()
GEMINI_API_KEY     = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
OPENROUTER_API_KEY = _s("OPENROUTER_API_KEY") or _s("OPENROUTER_KEY") or ""
COHERE_API_KEY     = _s("COHERE_API_KEY") or ""
EXTRA_API_KEY      = _s("EXTRA_API_KEY")


def any_ai_provider_configured() -> bool:
    """
    True إذا وُجد مفتاح لأي مزود يدعمه المحرك (Gemini مباشرة، أو OpenRouter، أو Cohere).
    على Railway يكفي OPENROUTER_API_KEY بدون GEMINI_* — المسارات تتدرج تلقائياً في ai_engine.
    """
    if GEMINI_API_KEYS:
        return True
    if (OPENROUTER_API_KEY or "").strip():
        return True
    if (COHERE_API_KEY or "").strip():
        return True
    return False


ANY_AI_PROVIDER_CONFIGURED = any_ai_provider_configured()

# ══════════════════════════════════════════════
#  Make Webhooks — يجب ضبطهما في متغيرات البيئة (Railway / .env)
#  لا يوجد fallback إنتاجي — أي إرسال بدون URL سيُعيد خطأ صريحاً
# ══════════════════════════════════════════════
WEBHOOK_UPDATE_PRICES = _s("WEBHOOK_UPDATE_PRICES")
WEBHOOK_NEW_PRODUCTS  = _s("WEBHOOK_NEW_PRODUCTS")

# ══════════════════════════════════════════════
#  ألوان
# ══════════════════════════════════════════════
COLORS = {
    "raise": "#dc3545", "lower": "#00C853", "approved": "#28a745",
    "missing": "#007bff", "review": "#ff9800", "excluded": "#9e9e9e",
    "primary": "#6C63FF",
}

# ══════════════════════════════════════════════
#  إعدادات المطابقة
# ══════════════════════════════════════════════
MATCH_THRESHOLD    = 85
HIGH_CONFIDENCE    = 95
REVIEW_THRESHOLD   = 75
PRICE_TOLERANCE    = 5
MIN_MATCH_SCORE    = MATCH_THRESHOLD
HIGH_MATCH_SCORE   = HIGH_CONFIDENCE
PRICE_DIFF_THRESHOLD = PRICE_TOLERANCE

# ══════════════════════════════════════════════
#  فلاتر المنتجات
# ══════════════════════════════════════════════
REJECT_KEYWORDS = [
    "sample","عينة","عينه","decant","تقسيم","تقسيمة",
    "split","miniature","0.5ml","1ml","2ml","3ml",
]
TESTER_KEYWORDS = ["tester","تستر","تيستر"]
SET_KEYWORDS    = ["set","gift set","طقم","مجموعة","coffret"]

# ══════════════════════════════════════════════
#  العلامات التجارية
# ══════════════════════════════════════════════
KNOWN_BRANDS = [
    "Dior","Chanel","Gucci","Tom Ford","Versace","Armani","YSL","Prada",
    "Burberry","Givenchy","Hermes","Creed","Montblanc","Calvin Klein",
    "Hugo Boss","Dolce & Gabbana","Valentino","Bvlgari","Cartier","Lancome",
    "Jo Malone","Amouage","Rasasi","Lattafa","Arabian Oud","Ajmal",
    "Al Haramain","Afnan","Armaf","Nishane","Xerjoff","Parfums de Marly",
    "Initio","Byredo","Le Labo","Mancera","Montale","Kilian","Roja",
    "Carolina Herrera","Jean Paul Gaultier","Narciso Rodriguez",
    "Paco Rabanne","Mugler","Chloe","Coach","Michael Kors","Ralph Lauren",
    "Maison Margiela","Memo Paris","Penhaligons","Serge Lutens","Diptyque",
    "Frederic Malle","Francis Kurkdjian","Floris","Clive Christian",
    "Ormonde Jayne","Zoologist","Tauer","Lush","The Different Company",
    "Missoni","Juicy Couture","Moschino","Dunhill","Bentley","Jaguar",
    "Boucheron","Chopard","Elie Saab","Escada","Ferragamo","Fendi",
    "Kenzo","Lacoste","Loewe","Rochas","Roberto Cavalli","Tiffany",
    "Van Cleef","Azzaro","Banana Republic","Benetton","Bottega Veneta",
    "Celine","Dsquared2","Ed Hardy","Elizabeth Arden","Ermenegildo Zegna",
    "Swiss Arabian","Ard Al Zaafaran","Nabeel","Asdaaf","Maison Alhambra",
    "لطافة","العربية للعود","رصاصي","أجمل","الحرمين","أرماف",
    "أمواج","كريد","توم فورد","ديور","شانيل","غوتشي","برادا",
    "ميسوني","جوسي كوتور","موسكينو","دانهيل","بنتلي",
    "كينزو","لاكوست","فندي","ايلي صعب","ازارو",
    "Guerlain","Givenchy","Sisley","Issey Miyake","Davidoff","Mexx",
]

# ══════════════════════════════════════════════
#  استبدالات التطبيع
# ══════════════════════════════════════════════
WORD_REPLACEMENTS = {
    'او دو بارفان':'edp','أو دو بارفان':'edp','او دي بارفان':'edp',
    'او دو تواليت':'edt','أو دو تواليت':'edt','او دي تواليت':'edt',
    'مل':'ml','ملي':'ml',
    'سوفاج':'sauvage','ديور':'dior','شانيل':'chanel',
    'توم فورد':'tom ford','أرماني':'armani','غيرلان':'guerlain',
}

# ══════════════════════════════════════════════
#  إعدادات الأتمتة الذكية v26.0
# ══════════════════════════════════════════════
AUTOMATION_RULES_DEFAULT = [
    {
        "name": "خفض السعر تلقائياً",
        "enabled": True,
        "condition": "our_price > comp_price",
        "min_diff": 10,       # فرق أدنى بالريال لتفعيل القاعدة
        "action": "undercut",  # خفض ليصبح أقل من المنافس
        "undercut_amount": 1,  # أقل بكم ريال
        "min_match_score": 90, # حد أدنى لنسبة التطابق
        "max_loss_pct": 15,    # أقصى نسبة خسارة مقبولة من سعر التكلفة
    },
    {
        "name": "رفع السعر عند فرصة ربح",
        "enabled": True,
        "condition": "our_price < comp_price",
        "min_diff": 15,
        "action": "raise_to_match",
        "margin_below": 5,     # أقل من المنافس بكم ريال
        "min_match_score": 90,
    },
    {
        "name": "إبقاء السعر إذا تنافسي",
        "enabled": True,
        "condition": "abs(our_price - comp_price) <= threshold",
        "threshold": 10,
        "action": "keep",
        "min_match_score": 85,
    },
]

# جدولة البحث الدوري (بالدقائق)
AUTO_SEARCH_INTERVAL_MINUTES = 60 * 6   # كل 6 ساعات
AUTO_PUSH_TO_MAKE = False               # إرسال تلقائي لـ Make.com (يتطلب تفعيل يدوي)
AUTO_DECISION_CONFIDENCE = 92           # حد الثقة للقرار التلقائي

# ══════════════════════════════════════════════
#  أقسام التطبيق (v26.0 — مع لوحة الأتمتة)
# ══════════════════════════════════════════════
SECTIONS = [
    "📊 لوحة التحكم",
    "🔴 سعر أعلى",
    "🟢 سعر أقل",
    "✅ موافق عليها",
    "🔍 منتجات مفقودة",
    "⚠️ تحت المراجعة",
    "⚪ مستبعد (لا يوجد تطابق)",
    "✔️ تمت المعالجة",
    "⚡ أتمتة Make",
    "🔄 الأتمتة الذكية",
    "🕷️ كشط المنافسين",
    "⚙️ الإعدادات",
]
SIDEBAR_SECTIONS = SECTIONS
PAGES_PER_TABLE  = 25
# DB_PATH محذوف — استخدم db_manager.DB_PATH الموحّد (pricing_v18.db)
