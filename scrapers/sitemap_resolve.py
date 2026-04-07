"""
scrapers/sitemap_resolve.py — حل روابط Sitemap v2.0 (2026)
═══════════════════════════════════════════════════════════════
يحدّد مسار Sitemap لأي متجر إلكتروني بأولوية:
  1. robots.txt → سطور Sitemap: (المصدر الأكثر شرعية)
  2. مسارات سلة / زد / Shopify / WooCommerce
  3. /sitemap.xml   /sitemap_index.xml  (المعيار العام)

يُعيد قائمة URLs لمنتجات المتجر جاهزة للكشط مع تاريخ آخر تعديل.

ملف Sitemap هو دليل علني تضعه المتاجر عمداً لمحركات البحث — وليس ثغرة أمنية.
"""
from __future__ import annotations

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp

from scrapers.anti_ban import get_xml_headers, get_browser_headers

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=30)

# ══════════════════════════════════════════════════════════════════════════
#  ثوابت ومسارات Sitemap
# ══════════════════════════════════════════════════════════════════════════
_SITEMAP_CANDIDATES = [
    "/sitemap_index.xml",
    "/sitemap.xml",
    "/sitemap-products.xml",
    "/products-sitemap.xml",
    "/sitemap_products.xml",
    "/page-sitemap.xml",
    "/product-sitemap.xml",
    "/sitemap1.xml",
]

_SALLA_EXTRA_PATHS = [
    "/sitemap.xml",
    "/sitemap_products.xml",
    "/sitemap-products.xml",
]

_ZID_EXTRA_PATHS = [
    "/sitemap.xml",
    "/sitemap_products.xml",
]

_SALLA_DOMAINS = re.compile(
    r"(salla\.sa|salla\.store|\.salla\.|s\.salla\.sa)", re.I
)
_ZID_DOMAINS = re.compile(
    r"(zid\.store|\.zid\.sa|zid\.sa)", re.I
)

# صفحة منتج — أنماط شائعة عبر المنصات
_PRODUCT_URL_RE = re.compile(
    r"(/p\d{5,}$"         # سلة: /p123456789
    r"|/products?/"       # Shopify / WooCommerce
    r"|/item/"
    r"|/shop/"
    r"|/ar/p/"
    r"|/en/p/"
    r"|/product-page/"
    r"|منتج"
    r")",
    re.I,
)

_EXCLUDE_URL_RE = re.compile(
    r"(/blog/|/page/|/category/|/categories/|/tag/|/cart|/checkout"
    r"|/account|/contact|/about|/faq|/privacy|/terms"
    r"|/cdn\.|\.js$|\.css$|\.png$|\.jpg$|\.webp$|\.svg$"
    r"|/feed/|/rss|/amp/)",
    re.I,
)


@dataclass
class SitemapEntry:
    """رابط منتج مع تاريخ آخر تعديل (اختياري)."""
    url: str
    lastmod: str = ""


@dataclass
class SitemapDiag:
    """تشخيص عملية حل الـ Sitemap — يُعرض في الواجهة."""
    store_url: str = ""
    robots_sitemaps: List[str] = field(default_factory=list)
    sitemap_found: str = ""
    urls_total: int = 0
    urls_product: int = 0
    errors: List[str] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════
#  دوال مساعدة
# ══════════════════════════════════════════════════════════════════════════
def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _is_salla(url: str) -> bool:
    return bool(_SALLA_DOMAINS.search(url))


def _is_zid(url: str) -> bool:
    return bool(_ZID_DOMAINS.search(url))


async def _fetch_xml(
    session: aiohttp.ClientSession, url: str
) -> Optional[str]:
    """GET مع رؤوس XML وتجاهل TLS — يُرجع نص XML أو None."""
    try:
        async with session.get(
            url,
            headers=get_xml_headers(),
            ssl=False,
            allow_redirects=True,
            timeout=_TIMEOUT,
        ) as resp:
            if resp.status == 200:
                ct = (resp.headers.get("Content-Type") or "").lower()
                if "xml" in ct or "text" in ct or url.endswith(".xml"):
                    return await resp.text(errors="ignore")
            logger.debug("_fetch_xml %s → HTTP %s", url, resp.status)
    except Exception as exc:
        logger.debug("_fetch_xml %s → %s", url, exc)
    return None


def _parse_sitemap_xml(xml_text: str) -> Tuple[List[SitemapEntry], List[str]]:
    """
    يحلل XML ويعيد:
      - entries: قائمة SitemapEntry (url + lastmod)
      - sub_sitemaps: روابط sitemapindex فرعية (تحتاج جلب إضافي)
    """
    entries: List[SitemapEntry] = []
    sub_sitemaps: List[str] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return entries, sub_sitemaps

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    root_tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if root_tag == "sitemapindex":
        for sitemap_el in root.findall(".//sm:sitemap", ns):
            loc = sitemap_el.find("sm:loc", ns)
            if loc is not None and loc.text:
                sub_sitemaps.append(loc.text.strip())
        if not sub_sitemaps:
            for el in root.iter():
                tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
                if tag == "loc" and el.text and el.text.strip().endswith(".xml"):
                    sub_sitemaps.append(el.text.strip())
        return entries, sub_sitemaps

    if root_tag == "urlset":
        for url_el in root.findall(".//sm:url", ns):
            loc = url_el.find("sm:loc", ns)
            lastmod_el = url_el.find("sm:lastmod", ns)
            if loc is not None and loc.text:
                entries.append(SitemapEntry(
                    url=loc.text.strip(),
                    lastmod=(lastmod_el.text.strip() if lastmod_el is not None and lastmod_el.text else ""),
                ))

    if not entries and not sub_sitemaps:
        for el in root.iter():
            tag = el.tag.split("}")[-1] if "}" in el.tag else el.tag
            if tag == "loc" and el.text:
                u = el.text.strip()
                if u.endswith(".xml"):
                    sub_sitemaps.append(u)
                elif u.startswith("http"):
                    entries.append(SitemapEntry(url=u))

    return entries, sub_sitemaps


_SITEMAP_CONCURRENCY = 10  # الحد الأقصى لطلبات sitemap المتزامنة في نفس الوقت


async def _fetch_and_parse_sitemap(
    session: aiohttp.ClientSession,
    url: str,
    depth: int = 0,
    max_depth: int = 3,
    sem: Optional[asyncio.Semaphore] = None,
) -> List[SitemapEntry]:
    """يجلب ويحلل sitemap (يتتبع sitemapindex بشكل متكرر حتى max_depth).
    يستخدم سيمافوراً لتقييد التزامن ومنع انفجار coroutines عند sitemapindex ضخم.
    """
    if depth > max_depth:
        return []

    # إنشاء السيمافور عند أول استدعاء — يُمرَّر للاستدعاءات الفرعية
    if sem is None:
        sem = asyncio.Semaphore(_SITEMAP_CONCURRENCY)

    async with sem:
        xml = await _fetch_xml(session, url)
        if not xml:
            return []

        if "<urlset" not in xml[:2000] and "<sitemapindex" not in xml[:2000]:
            return []

        entries, sub_sitemaps = _parse_sitemap_xml(xml)

    if sub_sitemaps:
        # تقسيم الطلبات الفرعية إلى دفعات لمنع إنشاء آلاف coroutines دفعة واحدة
        batch_size = _SITEMAP_CONCURRENCY
        for i in range(0, len(sub_sitemaps), batch_size):
            batch = sub_sitemaps[i:i + batch_size]
            tasks = [
                _fetch_and_parse_sitemap(session, sub_url, depth + 1, max_depth, sem)
                for sub_url in batch
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, list):
                    entries.extend(r)

    return entries


async def _sitemaps_from_robots(
    session: aiohttp.ClientSession, base: str
) -> List[str]:
    """يستخرج روابط Sitemap من robots.txt."""
    text = await _fetch_xml(session, f"{base}/robots.txt")
    if not text:
        return []
    found = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.lower().startswith("sitemap:"):
            url = stripped.split(":", 1)[1].strip()
            if url.startswith("http"):
                found.append(url)
    return found


def _is_product_url(url: str) -> bool:
    """هل الرابط يبدو صفحة منتج؟"""
    if _EXCLUDE_URL_RE.search(url):
        return False
    return bool(_PRODUCT_URL_RE.search(url))


def _is_salla_product(url: str) -> bool:
    """سلة: المنتجات عادة /p[0-9]+ أو تنتهي بمعرّف رقمي طويل."""
    path = urlparse(url).path or ""
    if re.search(r"/p\d{5,}$", path):
        return True
    if re.search(r"/\d{8,}$", path):
        return True
    if "/products/" in path.lower() or "/product/" in path.lower():
        return True
    return False


def _filter_product_entries(entries: List[SitemapEntry], base: str) -> List[SitemapEntry]:
    """يُبقي فقط صفحات المنتجات ويُزيل CDN/blog/static."""
    salla = _is_salla(base)
    product_entries: List[SitemapEntry] = []

    for e in entries:
        try:
            p = urlparse(e.url)
        except Exception:
            continue
        host = (p.netloc or "").lower()
        if "cdn." in host:
            continue
        if _EXCLUDE_URL_RE.search(e.url):
            continue

        if salla:
            if _is_salla_product(e.url):
                product_entries.append(e)
        elif _is_product_url(e.url):
            product_entries.append(e)

    return product_entries


# ══════════════════════════════════════════════════════════════════════════
#  الدالة الرئيسية
# ══════════════════════════════════════════════════════════════════════════
async def resolve_product_urls(
    store_url: str,
    session: aiohttp.ClientSession,
    *,
    max_products: int = 0,
) -> List[str]:
    """
    تُرجع قائمة URLs لصفحات المنتجات الجاهزة للكشط.

    max_products=0 → كل المنتجات بلا سقف.

    الخوارزمية:
    1. robots.txt → سطور Sitemap
    2. مسارات خاصة بسلة / زد / Shopify
    3. مسارات Sitemap المعيارية
    4. فلترة → صفحات منتجات فقط
    """
    base = _base_url(store_url)
    all_entries: List[SitemapEntry] = []

    # 1) robots.txt (الأشرع — هذا ما تعلنه المتاجر رسمياً)
    robots_urls = await _sitemaps_from_robots(session, base)
    for surl in robots_urls:
        entries = await _fetch_and_parse_sitemap(session, surl)
        all_entries.extend(entries)

    # 2) مسارات خاصة بالمنصة
    if not all_entries:
        extra_paths = []
        if _is_salla(base):
            extra_paths = _SALLA_EXTRA_PATHS
        elif _is_zid(base):
            extra_paths = _ZID_EXTRA_PATHS

        for path in extra_paths:
            candidate = f"{base}{path}"
            entries = await _fetch_and_parse_sitemap(session, candidate)
            if entries:
                all_entries.extend(entries)
                break

    # 3) مسارات معيارية
    if not all_entries:
        for path in _SITEMAP_CANDIDATES:
            candidate = f"{base}{path}"
            entries = await _fetch_and_parse_sitemap(session, candidate)
            if entries:
                all_entries.extend(entries)
                break

    # 4) Shopify /products.json API
    if not all_entries:
        all_entries.extend(await _fallback_shopify_api(session, base, max_products))

    # 5) HTML crawl of /products page
    if not all_entries:
        all_entries.extend(await _fallback_html_product_page(session, base))

    # إزالة التكرار مع الحفاظ على الترتيب
    seen = set()
    unique: List[SitemapEntry] = []
    for e in all_entries:
        if e.url not in seen:
            seen.add(e.url)
            unique.append(e)

    # فلترة → منتجات فقط
    product_entries = _filter_product_entries(unique, base)

    if not product_entries and unique:
        logger.info(
            "لا صفحات منتجات بعد الفلترة (%d رابط كلي) — يُرجع الكل", len(unique)
        )
        product_entries = unique

    # تطبيق السقف
    if max_products > 0:
        product_entries = product_entries[:max_products]

    logger.info(
        "resolve_product_urls %s → %d منتج (من %d رابط كلي)",
        base, len(product_entries), len(unique),
    )
    return [e.url for e in product_entries]


async def resolve_product_entries(
    store_url: str,
    session: aiohttp.ClientSession,
    *,
    max_products: int = 0,
) -> List[SitemapEntry]:
    """مثل resolve_product_urls لكن يُعيد SitemapEntry (url + lastmod) للكشط التزايدي."""
    base = _base_url(store_url)
    all_entries: List[SitemapEntry] = []

    robots_urls = await _sitemaps_from_robots(session, base)
    for surl in robots_urls:
        entries = await _fetch_and_parse_sitemap(session, surl)
        all_entries.extend(entries)

    if not all_entries:
        extra_paths = []
        if _is_salla(base):
            extra_paths = _SALLA_EXTRA_PATHS
        elif _is_zid(base):
            extra_paths = _ZID_EXTRA_PATHS

        for path in extra_paths:
            entries = await _fetch_and_parse_sitemap(session, f"{base}{path}")
            if entries:
                all_entries.extend(entries)
                break

    if not all_entries:
        for path in _SITEMAP_CANDIDATES:
            entries = await _fetch_and_parse_sitemap(session, f"{base}{path}")
            if entries:
                all_entries.extend(entries)
                break

    # ── Fallback 4: Shopify /products.json API ─────────────────────────────
    if not all_entries:
        all_entries.extend(await _fallback_shopify_api(session, base, max_products))

    # ── Fallback 5: crawl /products page for anchor hrefs ──────────────────
    if not all_entries:
        all_entries.extend(await _fallback_html_product_page(session, base))

    seen = set()
    unique: List[SitemapEntry] = []
    for e in all_entries:
        if e.url not in seen:
            seen.add(e.url)
            unique.append(e)

    product_entries = _filter_product_entries(unique, base)
    if not product_entries and unique:
        product_entries = unique

    if max_products > 0:
        product_entries = product_entries[:max_products]

    return product_entries


async def _fallback_shopify_api(
    session: aiohttp.ClientSession,
    base: str,
    max_products: int = 0,
) -> List[SitemapEntry]:
    """Shopify /products.json — صفحات متعددة حتى max_products."""
    entries: List[SitemapEntry] = []
    page = 1
    limit = 250
    try:
        while True:
            url = f"{base}/products.json?limit={limit}&page={page}"
            try:
                async with session.get(url, headers=get_browser_headers(), timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        break
                    data = await r.json(content_type=None)
            except Exception:
                break
            products = data.get("products") or []
            if not products:
                break
            for p in products:
                handle = p.get("handle", "")
                if handle:
                    entries.append(SitemapEntry(url=f"{base}/products/{handle}"))
            if len(products) < limit:
                break
            page += 1
            if max_products > 0 and len(entries) >= max_products:
                break
    except Exception:
        pass
    return entries


async def _fallback_html_product_page(
    session: aiohttp.ClientSession,
    base: str,
) -> List[SitemapEntry]:
    """
    يجلب صفحة /products أو الصفحة الرئيسية ويستخرج روابط المنتجات من <a href>.
    مناسب للمتاجر التي تعجز عن تقديم Sitemap.
    """
    entries: List[SitemapEntry] = []
    candidates_pages = ["/products", "/shop", "/store", "/"]
    _product_href_re = re.compile(
        r'href=["\']([^"\']*(?:/p\d{5,}|/products?/[^"\'/?#]{4,}|/item/[^"\'/?#]{4,}))["\']',
        re.I,
    )
    for path in candidates_pages:
        try:
            async with session.get(
                f"{base}{path}", headers=get_browser_headers(),
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status != 200:
                    continue
                html = await r.text(errors="ignore")
        except Exception:
            continue
        found = _product_href_re.findall(html)
        if not found:
            continue
        seen_local: set = set()
        for href in found:
            full = href if href.startswith("http") else f"{base}{href}"
            full = full.split("?")[0].rstrip("/")
            if full not in seen_local:
                seen_local.add(full)
                entries.append(SitemapEntry(url=full))
        if entries:
            logger.info("_fallback_html_product_page %s → %d روابط من %s", base, len(entries), path)
            break
    return entries


# ══════════════════════════════════════════════════════════════════════════
#  دالة مزامنة لتحليل رابط → Sitemap (تُستخدم في واجهة app.py)
# ══════════════════════════════════════════════════════════════════════════
def resolve_store_to_sitemap_url(user_input: str) -> Tuple[Optional[str], str]:
    """
    يعيد (رابط Sitemap الجاهز للكشط، رسالة توضيحية).
    إذا فشل يعيد (None, سبب).
    """
    import requests as _req

    raw = (user_input or "").strip()
    if not raw:
        return None, "الرجاء إدخال رابط."

    if not raw.lower().startswith(("http://", "https://")):
        raw = "https://" + raw
    p = urlparse(raw)
    if not p.netloc:
        return None, "تعذر قراءة نطاق الرابط."

    base = f"{p.scheme}://{p.netloc}"

    def _probe(url: str) -> bool:
        try:
            r = _req.get(url, headers=get_xml_headers(), timeout=20, allow_redirects=True)
            if r.status_code != 200:
                return False
            t = r.text.lstrip()[:2000]
            return bool(re.search(r"<(?:urlset|sitemapindex)\b", t, re.I))
        except Exception:
            return False

    # رابط مباشر لـ XML
    if p.path.lower().endswith(".xml"):
        if _probe(raw):
            return raw, f"تم اعتماد Sitemap مباشرة: `{raw}`"

    # robots.txt
    try:
        r = _req.get(
            f"{base}/robots.txt",
            headers=get_xml_headers(),
            timeout=15,
            allow_redirects=True,
        )
        if r.status_code == 200:
            for line in r.text.splitlines():
                if line.strip().lower().startswith("sitemap:"):
                    u = line.split(":", 1)[1].strip()
                    if u.startswith("http") and _probe(u):
                        return u, f"تم الاستنتاج من robots.txt: `{u}`"
    except Exception:
        pass

    # مسارات شائعة
    for path in _SITEMAP_CANDIDATES:
        candidate = f"{base}{path}"
        if _probe(candidate):
            return candidate, f"تم الاستنتاج تلقائياً: `{candidate}`"

    return (
        None,
        "لم يُعثر على Sitemap يعمل (HTTP 200 وXML). "
        "جرّب فتح الرابط في المتصفح أو أضف رابط sitemap يدوياً.",
    )
