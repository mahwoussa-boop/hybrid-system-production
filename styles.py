"""
styles.py - التصميم v20.0 — بطاقات محسنة + عرض المنافسين
"""
import html
import re
from urllib.parse import urlparse

from utils.data_helpers import first_image_url_string

_BAD_META = frozenset({"", "—", "-", "nan", "none", "na", "<na>"})


def _strip_mahally_local_prefix(s: str) -> str:
    """إزالة بادئة «محلي -» الشائعة في عناوين المتاجر."""
    t = str(s or "").strip()
    if not t:
        return t
    t = re.sub(r"^محلي\s*[-–—:،]\s*", "", t)
    t = re.sub(r"^محلي\s+", "", t).strip()
    return t


def _is_upload_filename(s: str) -> bool:
    """اسم ملف مرفوع (مفتاح comp_dfs) — لا يُعرض كاسم متجر."""
    sl = str(s).strip().lower()
    return bool(sl.endswith((".csv", ".xlsx", ".xls", ".tsv", ".ods")))


def _lazy_img_tag(src, width=40, height=40, alt="", loading="lazy"):
    """وسم صورة — lazy افتراضياً؛ eager للصور فوق الطي (مثل «تمت المعالجة»)."""
    if not (src or "").strip():
        return ""
    s = html.escape(first_image_url_string(str(src).strip()), quote=True)
    if not s:
        return ""
    a = html.escape(alt or "", quote=True)
    ld = "eager" if str(loading).lower().strip() == "eager" else "lazy"
    # onerror: إخفاء الصورة المكسورة وإظهار placeholder بدلاً منها
    err_js = (
        f"this.onerror=null;"
        f"this.style.display='none';"
        f"var p=document.createElement('div');"
        f"p.style='width:{width}px;height:{height}px;border-radius:6px;"
        f"background:#121c2e;border:1px dashed #2a3f5f;display:flex;"
        f"align-items:center;justify-content:center;color:#4a5c78;font-size:.8rem;flex-shrink:0';"
        f"p.textContent='—';"
        f"this.parentNode.insertBefore(p,this);"
    )
    return (
        f'<img src="{s}" alt="{a}" loading="{ld}" decoding="async" '
        f'onerror="{html.escape(err_js, quote=True)}" '
        f'style="width:{width}px;height:{height}px;object-fit:cover;'
        f'border-radius:6px;flex-shrink:0;opacity:1;visibility:visible;display:block"/>'
    )


lazy_img_tag = _lazy_img_tag


def _comp_url_footer(comp_url):
    """ذيل بطاقة: رابط صفحة المنتج عند المنافس (يفتح في تاب جديد)."""
    u = (comp_url or "").strip()
    if not u.startswith("http"):
        return ""
    uh = html.escape(u, quote=True)
    return (
        f'<div style="text-align:center;padding:6px 10px 8px;border-top:1px solid rgba(51,51,68,.55);'
        f'background:rgba(10,17,32,.85)">'
        f'<a href="{uh}" target="_blank" rel="noopener noreferrer" '
        f'style="display:inline-block;font-size:.8rem;color:#4fc3f7;font-weight:700;text-decoration:none">'
        f'🔗 عرض عند المنافس</a></div>'
    )


def _miss_card_link_title(name, brand, comp, comp_url, size="", ptype=""):
    """عنوان مقروء لبطاقة المفقود عندما يكون عمود الاسم مخزناً كرابطاً."""
    n = str(name or "").strip()
    nl = n.lower()
    if not nl.startswith("http://") and not nl.startswith("https://"):
        return n
    b = str(brand or "").strip()
    c = str(comp or "").strip()
    sz = str(size or "").strip()
    pt = str(ptype or "").strip()
    parts = []
    if b and b.lower() not in _BAD_META:
        parts.append(b)
    if c and c.lower() not in _BAD_META and not _is_upload_filename(c):
        parts.append(c)
    if len(parts) >= 2:
        return f"{parts[0]} · {parts[1]}"
    if len(parts) == 1:
        return parts[0]
    # الاسم كان رابطاً ولا يوجد ماركة+متجر كافية: صِف المنتج بالحجم/النوع إن وُجد
    desc = [x for x in (b, sz, pt) if x and x.lower() not in _BAD_META]
    if desc:
        return " · ".join(desc[:4])
    u = (comp_url or "").strip()
    if u.startswith("http"):
        try:
            host = urlparse(u).netloc.replace("www.", "")
            if host:
                return f"منتج — {host}"
        except Exception:
            pass
    return "🔗 عرض المنتج"


def _linked_display_text(text, url) -> str:
    """لا نعرض عنوان http كاملاً كنص الرابط — غالباً خطأ عمود (الاسم مكان الرابط)."""
    t_raw = str(text or "").strip()
    u = (url or "").strip()
    if not u.startswith("http"):
        return t_raw
    tl = t_raw.lower()
    if tl.startswith("http://") or tl.startswith("https://"):
        return "🔗 عرض المنتج"
    return t_raw


def _linked_product_title(text, url, *, color, font_size, font_weight="700"):
    """اسم المنتج كرابط إلى صفحة المنافس عند توفر URL."""
    t_raw = _linked_display_text(text, url)
    t = html.escape(t_raw, quote=False)
    u = (url or "").strip()
    if u.startswith("http"):
        uh = html.escape(u, quote=True)
        return (
            f'<a href="{uh}" target="_blank" rel="noopener noreferrer" '
            f'style="color:{color};font-weight:{font_weight};font-size:{font_size};'
            f'text-decoration:underline;text-underline-offset:3px">{t}</a>'
        )
    return f'<span style="color:{color};font-weight:{font_weight};font-size:{font_size}">{t}</span>'


linked_product_title = _linked_product_title


def get_styles():
    return get_main_css()

def get_main_css():
    return """<style>
@import url('https://fonts.googleapis.com/css2?family=Cairo:wght@400;600;700&family=Tajawal:wght@400;700;900&display=swap');
*{font-family:'Cairo','Tajawal',sans-serif!important}
.main .block-container{max-width:1400px;padding:.75rem 1.5rem}
.stat-card{background:#1A1A2E;border-radius:12px;padding:16px;text-align:center;border:1px solid #333344}
.stat-card:hover{box-shadow:0 4px 16px rgba(108,99,255,.15);border-color:#6C63FF}
.stat-card .num{font-size:2.2rem;font-weight:900;margin:4px 0}
.stat-card .lbl{font-size:.85rem;color:#8B8B8B}
.cmp-table{width:100%;border-collapse:separate;border-spacing:0;border-radius:8px;overflow:hidden;font-size:.88rem}
.cmp-table thead th{background:#16213e;color:#fff;padding:10px 8px;font-weight:700;text-align:center;border-bottom:2px solid #6C63FF;position:sticky;top:0;z-index:10}
.cmp-table tbody tr:nth-child(even){background:rgba(26,26,46,.4)}
.cmp-table tbody tr:hover{background:rgba(108,99,255,.1)!important}
.cmp-table td{padding:8px 6px;text-align:center;border-bottom:1px solid rgba(51,51,68,.4);vertical-align:middle}
.td-our{background:rgba(108,99,255,.06)!important;border-right:3px solid #6C63FF;text-align:right!important;font-weight:600;color:#B8B4FF;max-width:250px;word-wrap:break-word}
.td-comp{background:rgba(255,152,0,.06)!important;border-left:3px solid #ff9800;text-align:right!important;font-weight:600;color:#FFD180;max-width:250px;word-wrap:break-word}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.75rem;font-weight:700}
.b-high{background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744}
.b-med{background:rgba(255,214,0,.15);color:#FFD600;border:1px solid #FFD600}
.b-low{background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853}
.conf-bar{width:100%;height:6px;background:rgba(255,255,255,.08);border-radius:3px;overflow:hidden}
.conf-fill{height:100%;border-radius:3px}
/* ── منطقة الفلاتر المكشوفة ── */
.filter-inline-wrap{background:#12121f;border:1px solid #2a2a3d;border-radius:10px;padding:10px 12px;margin:6px 0 10px}
.filter-inline-title{font-size:.78rem;font-weight:700;color:#9e9e9e;margin-bottom:8px;letter-spacing:.02em}
/* ── بطاقة VS المحسنة مع المنافسين ── */
.vs-row{display:grid;grid-template-columns:1fr 36px 1fr;gap:10px;align-items:center;padding:12px;background:#1A1A2E;border-radius:8px 8px 0 0;margin:5px 0 0 0;border:1px solid #333344;border-bottom:none}
.vs-row.vs-compact{gap:8px;padding:8px;margin:3px 0 0 0}
.vs-row.vs-compact .vs-badge{width:28px;height:28px;font-size:.65rem}
.vs-badge{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.7rem}
.our-s{text-align:right;padding:8px;background:rgba(108,99,255,.04);border-radius:6px;border-right:3px solid #6C63FF}
.comp-s{text-align:left;padding:8px;background:rgba(255,152,0,.04);border-radius:6px;border-left:3px solid #ff9800}
.action-btn{display:inline-block;padding:4px 10px;border-radius:6px;font-size:.75rem;font-weight:700;cursor:pointer;margin:2px;border:1px solid}
.btn-approve{background:rgba(0,200,83,.1);color:#00C853;border-color:#00C853}
.btn-remove{background:rgba(255,23,68,.1);color:#FF1744;border-color:#FF1744}
.btn-delay{background:rgba(255,152,0,.1);color:#ff9800;border-color:#ff9800}
.btn-export{background:rgba(108,99,255,.1);color:#6C63FF;border-color:#6C63FF}
.ai-box{background:#1A1A2E;padding:12px;border-radius:8px;border:1px solid #333344;margin:6px 0}
.paste-area{background:#0E1117;border:2px dashed #333344;border-radius:8px;padding:12px;min-height:80px}
.multi-comp{background:rgba(0,123,255,.06);border:1px solid rgba(0,123,255,.2);border-radius:6px;padding:8px;margin:4px 0}
/* ── شريط المنافسين المصغر ── */
.comp-strip{background:#0e1628;border:1px solid #333344;border-top:none;border-radius:0 0 8px 8px;padding:8px 12px;margin:0 0 2px 0;display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.comp-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:14px;font-size:.72rem;font-weight:600;border:1px solid;white-space:nowrap}
.comp-chip.leader{background:rgba(255,152,0,.12);border-color:#ff9800;color:#ffb74d}
.comp-chip.normal{background:rgba(108,99,255,.08);border-color:#333366;color:#9e9eff}
.comp-chip .cp-name{max-width:100px;overflow:hidden;text-overflow:ellipsis}
.comp-chip .cp-price{font-weight:900}
/* ── بطاقة المنتج المفقود المحسنة ── */
.miss-card{border-radius:10px;padding:14px;margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30)}
.miss-card .miss-header{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.miss-card .miss-info{flex:1}
.miss-card .miss-name{font-weight:700;color:#4fc3f7;font-size:1rem}
.miss-card .miss-meta{font-size:.75rem;color:#888;margin-top:4px}
.miss-card .miss-prices{text-align:left;min-width:120px}
.miss-card .miss-comp-price{font-size:1.2rem;font-weight:900;color:#ff9800}
.miss-card .miss-suggested{font-size:.72rem;color:#4caf50}
/* ── شارات الثقة ── */
.trust-badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.68rem;font-weight:700;margin-right:4px}
.trust-green{background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C85366}
.trust-yellow{background:rgba(255,214,0,.15);color:#FFD600;border:1px solid #FFD60066}
.trust-red{background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF174466}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#0E1117,#1A1A2E);transition:all .3s ease}
#MainMenu,footer{visibility:hidden}
/* header يبقى ظاهراً لأنه يحتوي على زر إظهار القائمة الجانبية */
header[data-testid="stHeader"] {
    background: transparent !important;
    backdrop-filter: none !important;
}
/* إصلاح أيقونات Streamlit */
[data-testid="stExpander"] summary svg,
[data-testid="stSelectbox"] svg[data-testid="stExpanderToggleIcon"],
details summary span[data-testid] svg {
    font-family: system-ui, -apple-system, sans-serif !important;
}
[data-testid="stExpander"] summary {
    direction: rtl;
    font-family: 'Tajawal', sans-serif !important;
}
.stSelectbox label, .stMultiSelect label {
    direction: rtl;
    font-family: 'Tajawal', sans-serif !important;
}
/* ── زر القائمة الجانبية ── منقول إلى get_sidebar_toggle_js */
</style>"""


def get_sidebar_toggle_js():
    """CSS فقط لزر إخفاء/إظهار القائمة الجانبية — متوافق مع Streamlit Cloud"""
    return """<style>
/* زر إخفاء/إظهار القائمة الجانبية — يستخدم الزر المدمج في Streamlit */
[data-testid="collapsedControl"] {
    color: #6C63FF !important;
    background: linear-gradient(180deg,#6C63FF22,#4a42cc22) !important;
    border: 1px solid #6C63FF44 !important;
    border-radius: 0 8px 8px 0 !important;
    transition: all .25s ease !important;
}
[data-testid="collapsedControl"]:hover {
    background: linear-gradient(180deg,#6C63FF44,#4a42cc44) !important;
    box-shadow: 3px 0 10px rgba(108,99,255,.4) !important;
}
</style>
"""


def stat_card(icon, label, value, color="#6C63FF"):
    return f'<div class="stat-card" style="border-top:3px solid {color}"><div style="font-size:1.3rem">{icon}</div><div class="num" style="color:{color}">{value}</div><div class="lbl">{label}</div></div>'


def vs_card(our_name, our_price, comp_name, comp_price, diff, comp_source="", product_id="",
            our_img="", comp_img="", comp_url="", our_url="",
            accent_border=None, row_bg=None, compact=False, price_alert=""):
    """بطاقة VS الأساسية — المنافس الرئيسي (الأقل سعراً). our_img/comp_img/comp_url/our_url اختياريان.
    accent_border/row_bg: تنسيق اختياري لصفوف «مستبعد» (رمادي).
    compact: وضع أصغر لقسم «سعر أعلى» (فرصة خفض).
    price_alert: تنبيه تغيير سعر المنافس من الرادار التسعيري."""
    dc = "#FF1744" if diff > 0 else "#00C853" if diff < 0 else "#FFD600"
    src = f'<div style="font-size:.65rem;color:#666">{comp_source}</div>' if comp_source else ""
    pid = str(product_id) if product_id and str(product_id) not in ("", "nan", "None", "0") else ""
    pid_html = (
        f'<div style="display:inline-block;margin-top:3px;padding:1px 6px;'
        f'background:rgba(108,99,255,.18);border:1px solid rgba(108,99,255,.45);'
        f'border-radius:4px;font-size:.7rem;color:#a09dff;font-weight:600;'
        f'letter-spacing:.4px">#{pid}</div>'
    ) if pid else (
        f'<div style="display:inline-block;margin-top:3px;padding:1px 6px;'
        f'background:rgba(255,71,87,.12);border:1px solid rgba(255,71,87,.35);'
        f'border-radius:4px;font-size:.65rem;color:#ff6b6b88">لا يوجد رقم</div>'
    )
    _iw = _ih = (56 if compact else 90)
    _fs_our = ".82rem" if compact else ".9rem"
    _fs_price = "1rem" if compact else "1.1rem"
    our_thumb = _lazy_img_tag(our_img, width=_iw, height=_ih, alt="منتجنا")
    comp_thumb = _lazy_img_tag(comp_img, width=_iw, height=_ih, alt="المنافس")
    if not comp_thumb:
        comp_thumb = (
            f'<div style="width:{_iw}px;height:{_ih}px;border-radius:6px;flex-shrink:0;'
            f'background:#121c2e;border:1px dashed #2a3f5f;display:flex;align-items:center;'
            f'justify-content:center;color:#4a5c78;font-size:.8rem">—</div>'
        )
    # السعر المقترح = أقل من أقل منافس بريال
    suggested = comp_price - 1 if comp_price > 0 else 0
    sugg_html = ""
    if suggested > 0 and diff > 10:
        sugg_html = f'<div style="font-size:.7rem;color:#4caf50;margin-top:2px">مقترح: {suggested:,.0f} ر.س</div>'
    our_title_html = _linked_product_title(
        our_name, our_url, color="#B8B4FF", font_size=_fs_our,
    )
    our_block = (
        f'<div class="our-s" style="display:flex;align-items:flex-start;gap:10px;flex-direction:row-reverse">'
        f'{our_thumb}<div style="flex:1;min-width:0">'
        f'<div style="font-size:.7rem;color:#8B8B8B">منتجنا</div>'
        f'<div style="line-height:1.35">{our_title_html}</div>{pid_html}'
        f'<div style="font-size:{_fs_price};font-weight:900;color:#6C63FF;margin-top:2px">{our_price:.0f} ر.س</div>{sugg_html}</div></div>'
    )
    comp_title_html = _linked_product_title(
        comp_name, comp_url, color="#FFD180", font_size=_fs_our,
    )
    # data-img: رابط صورة المنافس — للتشخيص عبر DOM بدون إظهار نص
    _comp_img_attr = f' data-img="{html.escape(str(comp_img or ""), quote=True)}"' if comp_img else ' data-img=""'
    comp_block = (
        f'<div class="comp-s"{_comp_img_attr} style="display:flex;align-items:flex-start;gap:10px">'
        f'{comp_thumb}<div style="flex:1;min-width:0">'
        f'<div style="font-size:.7rem;color:#8B8B8B">المنافس المتصدر</div>'
        f'<div style="line-height:1.35">{comp_title_html}</div>'
        f'<div style="font-size:{_fs_price};font-weight:900;color:#ff9800;margin-top:2px">{comp_price:.0f} ر.س</div>{src}</div></div>'
    )
    _foot = _comp_url_footer(comp_url)
    _vs_cls = "vs-row vs-compact" if compact else "vs-row"
    _diff_fs = ".82rem" if compact else ".9rem"
    # ── شريط تنبيه الرادار التسعيري ──────────────────────────────────────
    _alert_html = ""
    _pa = str(price_alert or "").strip()
    if _pa:
        _is_drop = _pa.startswith("📉")
        _alert_bg  = "rgba(0,200,83,.12)"  if _is_drop else "rgba(255,71,87,.12)"
        _alert_bdr = "#00C853"             if _is_drop else "#FF1744"
        _alert_html = (
            f'<div style="background:{_alert_bg};border:1px solid {_alert_bdr};'
            f'border-radius:6px;padding:5px 10px;margin:4px 0;'
            f'font-size:.78rem;font-weight:700;color:{"#00C853" if _is_drop else "#FF5252"};'
            f'text-align:center;letter-spacing:.3px">🚨 رادار التسعير: {html.escape(_pa)}</div>'
        )
    inner = f'''<div class="{_vs_cls}">
{our_block}
<div class="vs-badge">VS</div>
{comp_block}
</div><div style="text-align:center;background:#1A1A2E;padding:3px;border-left:1px solid #333344;border-right:1px solid #333344;margin:0"><span style="color:{dc};font-weight:700;font-size:{_diff_fs}">الفرق: {diff:+.0f} ر.س</span></div>{_alert_html}{_foot}'''
    if accent_border:
        bg = row_bg if row_bg is not None else "rgba(158,158,158,.10)"
        return (
            f'<div style="border-top:3px solid {accent_border};background:{bg};'
            f'border-radius:10px;padding:6px 6px 0;margin:8px 0">{inner}</div>'
        )
    return inner


def comp_strip(all_comps):
    """شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم واسم المنتج لديهم مرتبين من الأقل"""
    if not all_comps or not isinstance(all_comps, list) or len(all_comps) == 0:
        return ""
    # ترتيب من الأقل سعراً
    sorted_comps = sorted(all_comps, key=lambda c: float(c.get("price", 0) or 0))
    rows = []
    for i, cm in enumerate(sorted_comps):
        c_store = str(cm.get("competitor", "")).strip()
        c_price = float(cm.get("price", 0) or 0)
        c_pname = str(cm.get("name", "")).strip()
        c_score = float(cm.get("score", 0) or 0)
        is_leader = (i == 0)
        crown = "👑" if is_leader else ""
        bg = "rgba(255,152,0,.10)" if is_leader else "rgba(108,99,255,.05)"
        border = "#ff9800" if is_leader else "#333366"
        name_color = "#ffb74d" if is_leader else "#9e9eff"
        # اسم المنتج لدى المنافس (مختصر)
        short_pname = c_pname[:50] + ".." if len(c_pname) > 50 else c_pname
        score_html = f'<span style="color:#888;font-size:.62rem">{c_score:.0f}%</span>' if c_score > 0 else ""
        _thumb_url = (cm.get("thumb") or cm.get("image_url") or cm.get("image") or "")
        thumb_html = _lazy_img_tag(_thumb_url, width=24, height=24, alt="") if _thumb_url else ""
        purl = str(cm.get("product_url") or cm.get("url") or "").strip()
        title_esc = html.escape(c_pname, quote=True)
        if purl.startswith("http"):
            pu = html.escape(purl, quote=True)
            pname_html = (
                f'<a href="{pu}" target="_blank" rel="noopener noreferrer" '
                f'style="color:#aaa;font-size:.7rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'
                f'max-width:300px;display:inline-block;text-decoration:underline;text-underline-offset:2px" '
                f'title="{title_esc}">{html.escape(short_pname, quote=False)}</a>'
            )
        else:
            pname_html = (
                f'<span style="color:#aaa;font-size:.7rem;overflow:hidden;text-overflow:ellipsis;'
                f'white-space:nowrap;max-width:300px" title="{title_esc}">'
                f'{html.escape(short_pname, quote=False)}</span>'
            )
        rows.append(
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:5px 10px;background:{bg};border:1px solid {border};border-radius:8px;'
            f'margin:2px 0;gap:8px;flex-wrap:wrap">'
            f'<div style="display:flex;align-items:center;gap:6px;flex:1;min-width:0">'
            f'{thumb_html}'
            f'<span style="font-weight:900;font-size:.8rem">{crown}</span>'
            f'<span style="font-weight:700;color:{name_color};font-size:.75rem;white-space:nowrap">{html.escape(c_store, quote=False)}</span>'
            f'{pname_html}'
            f'{score_html}'
            f'</div>'
            f'<span style="font-weight:900;color:{"#ff9800" if is_leader else "#9e9eff"};font-size:.85rem;white-space:nowrap">{c_price:,.0f} ر.س</span>'
            f'</div>'
        )
    return f'<div class="comp-strip" style="flex-direction:column;gap:2px">{chr(10).join(rows)}</div>'


def miss_card(name, price, brand, size, ptype, comp, suggested_price,
              note="", variant_html="", tester_badge="", border_color="#007bff44",
              confidence_level="green", confidence_score=0, product_id="", image_url="",
              comp_url="", title_override="", gray_zone_html="", dup_compare_html=""):
    """بطاقة المنتج المفقود — image_url / comp_url اختياريان.
    title_override: اسم عرض صريح (مثلاً من عمود آخر عندما يكون الاسم مخزناً كرابط)."""
    # شارة الثقة
    trust_map = {
        "green":  ("trust-green",  "مؤكد"),
        "yellow": ("trust-yellow", "محتمل"),
        "red":    ("trust-red",    "مشكوك"),
    }
    t_cls, t_lbl = trust_map.get(confidence_level, ("trust-green", "مؤكد"))
    trust_html = f'<span class="trust-badge {t_cls}">{t_lbl}</span>' if confidence_level != "green" else ""

    note_html = f'<div style="font-size:.72rem;color:#ff9800;margin-top:4px">{note}</div>' if note and "⚠️" in note else ""

    # عرض المعرف فقط إن كان حقيقياً — لا تكرار اسم المنتج كـ «رمز»
    pid_html = ""
    _name_s = str(name).strip()
    _pid_s = str(product_id).strip() if product_id else ""
    if (
        _pid_s
        and _pid_s not in ("nan", "none", "0")
        and _pid_s.lower() != _name_s.lower()
        and len(_pid_s) <= 64
    ):
        _pid_show = html.escape(_pid_s, quote=False)
        pid_html = (
            f'<span style="font-size:.7rem;padding:2px 8px;border-radius:8px;'
            f'background:#1a237e44;color:#90caf9;margin-right:6px;font-family:monospace;letter-spacing:1px">'
            f"📌 {_pid_show}</span>&nbsp;"
        )

    _ov = str(title_override or "").strip()
    if _ov and not _ov.lower().startswith(("http://", "https://")):
        _link_title = _ov
    else:
        _link_title = _miss_card_link_title(name, brand, comp, comp_url, size=size, ptype=ptype)
    _link_title = _strip_mahally_local_prefix(_link_title)
    _alt_name = (_link_title[:80] if _link_title else "")[:40]
    miss_img = _lazy_img_tag(image_url, width=72, height=72, alt=_alt_name) if image_url else ""
    miss_img_block = (
        f'<div style="flex-shrink:0;margin-left:10px">{miss_img}</div>' if miss_img else ""
    )

    _nm_html = _linked_product_title(_link_title, comp_url, color="#4fc3f7", font_size=".95rem")
    _br = html.escape(str(brand or "—"), quote=False)
    _sz = html.escape(str(size or "—"), quote=False)
    _pt = html.escape(str(ptype or "—"), quote=False)
    _cp = html.escape(str(comp), quote=False)
    # الرابط يبقى في عنوان المنتج القابل للنقر — بدون شريط إضافي أسفل البطاقة
    _foot = ""

    # بدون مسافات بادئة في بداية الأسطر — وإلا Markdown يعرضها كـ <pre><code>
    return (
        f'<div class="miss-card" style="border:1px solid {border_color}">'
        f'<div class="miss-header">'
        f"{miss_img_block}"
        f'<div class="miss-info">'
        f'<div class="miss-name">{trust_html}{tester_badge}{pid_html}{_nm_html}</div>'
        f'<div class="miss-meta">🏷️ {_br} &nbsp;|&nbsp; 📏 {_sz} &nbsp;|&nbsp; '
        f"🧴 {_pt} &nbsp;|&nbsp; 🏪 {_cp}</div>"
        f"{variant_html}"
        f"{note_html}"
        f"{gray_zone_html}"
        f"{dup_compare_html}"
        f"</div>"
        f'<div class="miss-prices">'
        f'<div class="miss-comp-price">{price:,.0f} ر.س</div>'
        f'<div class="miss-suggested">مقترح: {suggested_price:,.0f} ر.س</div>'
        f"</div></div>{_foot}</div>"
    )
