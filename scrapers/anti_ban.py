"""
scrapers/anti_ban.py — ترسانة ضد الحظر v3.0 (2026)
═══════════════════════════════════════════════════════
آليات متعددة الطبقات — Zero Bans:
  1. User-Agent ذكي — إصدارات 2026 من Chrome/Firefox/Safari/Edge
  2. Headers تحاكي المتصفح الحقيقي (Sec-CH-UA + TLS fingerprint)
  3. Rotating Proxies — تدوير IP مع كل طلب [جديد v3]
  4. Smart Jittering — تأخير عشوائي 1.5–3.2 ثانية بين الطلبات [محدَّث v3]
  5. Adaptive Rate Limiting — يبطّئ تلقائياً عند 429/403
  6. Exponential Backoff مع Jitter (3 محاولات قبل الاستسلام)
  7. curl_cffi كـ fallback أساسي (TLS fingerprint حقيقي)
  8. cloudscraper كـ fallback ثانوي
  9. Per-domain throttling + cookie persistence

إعداد الـ Proxies:
  export PROXY_LIST="http://user:pass@ip1:port,http://user:pass@ip2:port"
  إذا فارغة → الطلبات بدون proxy (graceful degradation)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
import threading
import time
from collections import defaultdict
from typing import List, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════
#  1. User-Agents — قاعدة بيانات حقيقية من متصفحات 2026
# ══════════════════════════════════════════════════════════════════════════
_REAL_UA_POOL = [
    # Chrome 134/133/132 — Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Chrome 134/133 — macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Firefox 135/134
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:135.0) Gecko/20100101 Firefox/135.0",
    # Safari 18
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    # Edge 134
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0",
    # Mobile Chrome (Android 15)
    "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.6998.99 Mobile Safari/537.36",
    # Mobile Safari (iOS 18)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1",
    # Googlebot — يُقبل دائماً من المتاجر لأنه يُستخدم لأرشفة المنتجات
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
]

_ACCEPT_LANGUAGES = [
    "ar-SA,ar;q=0.9,en-US;q=0.8,en;q=0.7",
    "ar,en-US;q=0.9,en;q=0.8",
    "en-US,en;q=0.9,ar;q=0.8",
    "ar-SA,ar;q=0.8,en;q=0.5",
]

_ACCEPT_HEADERS = [
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
]


def get_browser_headers(referer: str = "") -> dict:
    """يولّد headers تحاكي متصفحاً حقيقياً بالكامل — تتغير كل مرة."""
    ua = random.choice(_REAL_UA_POOL)
    headers = {
        "User-Agent":      ua,
        "Accept":          random.choice(_ACCEPT_HEADERS),
        "Accept-Language":  random.choice(_ACCEPT_LANGUAGES),
        "Accept-Encoding":  "gzip, deflate",
        "Connection":       "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":   "document",
        "Sec-Fetch-Mode":   "navigate",
        "Sec-Fetch-Site":   "none" if not referer else "cross-site",
        "Sec-Fetch-User":   "?1",
        "Cache-Control":    "max-age=0",
        "DNT":              "1",
    }
    if referer:
        headers["Referer"] = referer
        headers["Sec-Fetch-Site"] = "cross-site"
    # Chrome-style sec-ch-ua
    if "Chrome" in ua and "Edg" not in ua:
        major = ua.split("Chrome/")[1].split(".")[0] if "Chrome/" in ua else "134"
        headers.update({
            "sec-ch-ua":          f'"Chromium";v="{major}", "Google Chrome";v="{major}", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile":   "?0" if "Mobile" not in ua else "?1",
            "sec-ch-ua-platform": '"Windows"' if "Windows" in ua else ('"macOS"' if "Mac" in ua else '"Android"'),
        })
    elif "Edg" in ua:
        major = ua.split("Edg/")[1].split(".")[0] if "Edg/" in ua else "134"
        headers.update({
            "sec-ch-ua":          f'"Chromium";v="{major}", "Microsoft Edge";v="{major}", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile":   "?0",
            "sec-ch-ua-platform": '"Windows"',
        })
    return headers


def get_xml_headers() -> dict:
    """رؤوس خاصة بطلبات Sitemap XML — تطلب XML صراحة وتحاكي Googlebot."""
    ua = random.choice([
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    ])
    return {
        "User-Agent": ua,
        "Accept": "application/xml,text/xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ar-SA,ar;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache",
    }


# ══════════════════════════════════════════════════════════════════════════
#  2. Rotating Proxies — v3 جديد
# ══════════════════════════════════════════════════════════════════════════
class ProxyRotator:
    """
    يُدير قائمة الـ Proxies ويُدوّرها Round-Robin مع كل طلب.

    الإعداد عبر متغير البيئة:
        PROXY_LIST=http://user:pass@ip1:port,http://user:pass@ip2:port

    السلوك:
    - إذا فشل proxy ثلاث مرات → يُعطَّل مؤقتاً
    - إذا جميع الـ proxies معطَّلة → إعادة تعيين وإعادة المحاولة
    - إذا لا proxies مُعرَّفة → الطلبات مباشرة (graceful degradation)
    """

    def __init__(self) -> None:
        raw = os.environ.get("PROXY_LIST", "")
        self._proxies: List[str] = [p.strip() for p in raw.split(",") if p.strip()]
        self._index = 0
        self._failures: dict[str, int] = {}
        # قفل خفيف — آمن للاستخدام من خيوط متعددة وcoroutines
        self._lock = threading.Lock()
        if self._proxies:
            logger.info("ProxyRotator: %d proxy مُحمَّل", len(self._proxies))
        else:
            logger.debug("ProxyRotator: لا proxies مُعرَّفة — طلبات مباشرة")

    def get_next(self) -> Optional[str]:
        """يُرجع الـ proxy التالي (Round-Robin) أو None إذا لا proxies."""
        with self._lock:
            if not self._proxies:
                return None
            # الـ proxies النشطة (أقل من 3 فشل)
            active = [p for p in self._proxies if self._failures.get(p, 0) < 3]
            if not active:
                # جميعها فشلت → إعادة تعيين وإعادة المحاولة من الصفر
                self._failures.clear()
                active = self._proxies
                logger.warning("ProxyRotator: جميع الـ proxies فشلت — إعادة ضبط الدورة")
            proxy = active[self._index % len(active)]
            self._index = (self._index + 1) % max(len(active), 1)
            return proxy

    def mark_failed(self, proxy: str) -> None:
        """يُسجّل فشل proxy — بعد 3 فشل يُعطَّل."""
        with self._lock:
            self._failures[proxy] = self._failures.get(proxy, 0) + 1
            if self._failures[proxy] >= 3:
                logger.warning("Proxy معطَّل مؤقتاً (3 فشل): %s", proxy.split("@")[-1])

    def mark_success(self, proxy: str) -> None:
        """نجاح → إعادة تعيين عداد الفشل لهذا الـ proxy."""
        with self._lock:
            self._failures.pop(proxy, None)

    @property
    def has_proxies(self) -> bool:
        return bool(self._proxies)

    @property
    def active_count(self) -> int:
        return len([p for p in self._proxies if self._failures.get(p, 0) < 3])


# Singleton — مُشترَك بين جميع الـ coroutines
_proxy_rotator = ProxyRotator()


def get_proxy_rotator() -> ProxyRotator:
    """يُرجع نسخة ProxyRotator المُشتركة (Singleton)."""
    return _proxy_rotator


# ══════════════════════════════════════════════════════════════════════════
#  3. Adaptive Rate Limiter — Smart Jittering 1.5–3.2 ثانية
# ══════════════════════════════════════════════════════════════════════════
class AdaptiveRateLimiter:
    """
    يتتبع معدل الطلبات لكل domain ويضبطه تلقائياً:
    - تأخير أساسي: 1.5–3.2 ثانية (Smart Jittering — يصعب كشف الأنماط)
    - 429 Too Many Requests  → تضاعف وقت الانتظار (Exponential Backoff)
    - 403 Forbidden          → توقف مؤقت طويل + تغيير UA
    - 200 متواصلة            → تقليص الانتظار تدريجياً (Speed Up)
    """

    # حدود الجيتر الثابتة (المتطلبات: 1.5–3.2 ثانية)
    _JITTER_MIN = 1.5
    _JITTER_MAX = 3.2

    def __init__(self):
        self._state: dict[str, dict] = defaultdict(lambda: {
            "delay":          random.uniform(self._JITTER_MIN, self._JITTER_MAX),
            "consecutive_ok": 0,
            "backing_off":    False,
            "backoff_until":  0.0,
        })

    async def wait(self, domain: str) -> None:
        s = self._state[domain]
        now = time.monotonic()
        if s["backing_off"] and now < s["backoff_until"]:
            wait_t = s["backoff_until"] - now
            logger.debug("domain=%s backing-off %.1fs", domain, wait_t)
            await asyncio.sleep(wait_t)
            s["backing_off"] = False
        else:
            # Smart Jitter: 1.5–3.2 ثانية + انحراف عشوائي صغير
            jitter = random.uniform(self._JITTER_MIN, self._JITTER_MAX)
            # تطبيق حد أدنى يتكيف مع حالة الدومين
            effective_delay = max(jitter, s["delay"] * 0.6)
            await asyncio.sleep(effective_delay)

    def record_success(self, domain: str) -> None:
        s = self._state[domain]
        s["consecutive_ok"] += 1
        s["backing_off"] = False
        # تسريع تدريجي بعد 10 نجاحات متتالية (لكن لا نقل عن الحد الأدنى 1.5s)
        if s["consecutive_ok"] >= 10 and s["delay"] > self._JITTER_MIN:
            s["delay"] = max(self._JITTER_MIN, s["delay"] * 0.9)

    def record_error(self, domain: str, status: int) -> None:
        s = self._state[domain]
        s["consecutive_ok"] = 0
        if status == 429:
            backoff = min(s["delay"] * 3, 60.0) + random.uniform(5, 15)
            s["delay"] = min(s["delay"] * 2.5, 30.0)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("429 من %s — توقف %.0f ثانية (delay=%.1fs)", domain, backoff, s["delay"])
        elif status == 403:
            backoff = random.uniform(20, 60)
            s["backing_off"] = True
            s["backoff_until"] = time.monotonic() + backoff
            logger.warning("403 من %s — توقف %.0f ثانية", domain, backoff)
        elif status in (500, 502, 503, 504):
            s["delay"] = min(s["delay"] * 1.5, 15.0)


_rate_limiter = AdaptiveRateLimiter()


def get_rate_limiter() -> AdaptiveRateLimiter:
    return _rate_limiter


# ══════════════════════════════════════════════════════════════════════════
#  4. Retry مع Exponential Backoff + Proxy Rotation
# ══════════════════════════════════════════════════════════════════════════
async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_retries: int = 3,
    base_delay:  float = 2.0,
    referer:     str = "",
) -> Optional[aiohttp.ClientResponse]:
    """
    يجلب URL مع:
    - إعادة محاولة 3 مرات (Exponential Backoff)
    - تغيير headers + User-Agent في كل محاولة
    - تدوير Proxy في كل محاولة (إذا مُعرَّف)
    """
    domain = urlparse(url).netloc
    rl = get_rate_limiter()
    pr = get_proxy_rotator()

    for attempt in range(max_retries):
        headers = get_browser_headers(referer=referer or f"https://{domain}/")
        proxy = pr.get_next() if pr.has_proxies else None
        try:
            await rl.wait(domain)
            req_kwargs: dict = dict(headers=headers, ssl=False, allow_redirects=True)
            if proxy:
                req_kwargs["proxy"] = proxy

            resp = await session.get(url, **req_kwargs)

            if resp.status == 200:
                rl.record_success(domain)
                if proxy:
                    pr.mark_success(proxy)
                return resp

            rl.record_error(domain, resp.status)
            if proxy and resp.status in (403, 407, 502):
                pr.mark_failed(proxy)

            if resp.status in (404, 410):
                return None
            if resp.status in (429, 403, 500, 502, 503):
                delay = base_delay * (2 ** attempt) + random.uniform(1.5, 4.0)
                logger.debug(
                    "attempt %d/%d → HTTP %d — انتظار %.1fs (proxy=%s)",
                    attempt + 1, max_retries, resp.status, delay,
                    proxy.split("@")[-1] if proxy else "مباشر",
                )
                await asyncio.sleep(delay)
                continue

            return None

        except (aiohttp.ClientConnectorError, asyncio.TimeoutError) as exc:
            if proxy:
                pr.mark_failed(proxy)
            delay = base_delay * (2 ** attempt) + random.uniform(1.0, 3.0)
            logger.debug("attempt %d/%d: %s — انتظار %.1fs", attempt + 1, max_retries, exc, delay)
            await asyncio.sleep(delay)
        except Exception as exc:
            logger.debug("fetch_with_retry unexpected: %s", exc)
            return None

    return None


# ══════════════════════════════════════════════════════════════════════════
#  5. curl_cffi — TLS Fingerprint حقيقي (يتجاوز Cloudflare/Akamai)
# ══════════════════════════════════════════════════════════════════════════
def try_curl_cffi(url: str, timeout: int = 25, proxy: Optional[str] = None) -> Optional[str]:
    """
    يحاول جلب الصفحة عبر curl_cffi الذي ينتحل بصمة TLS لـ Chrome الحقيقي.
    أحدث وأنجح من cloudscraper لأنه يستخدم libcurl مع impersonation.
    """
    try:
        from curl_cffi import requests as cffi_requests
        req_kwargs: dict = dict(
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
        )
        if proxy:
            req_kwargs["proxies"] = {"http": proxy, "https": proxy}
        resp = cffi_requests.get(url, **req_kwargs)
        if resp.status_code == 200:
            return resp.text
        if resp.status_code in (403, 429) and proxy:
            _proxy_rotator.mark_failed(proxy)
    except ImportError:
        logger.debug("curl_cffi غير مثبّت — تخطّى")
    except Exception as exc:
        logger.debug("curl_cffi %s: %s", url, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  6. cloudscraper — Fallback ثانوي لـ Cloudflare JS Challenge
# ══════════════════════════════════════════════════════════════════════════
def try_cloudscraper(url: str, proxy: Optional[str] = None) -> Optional[str]:
    """يحاول جلب الصفحة عبر cloudscraper (يتجاوز JS Challenge)."""
    try:
        import cloudscraper
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        req_kwargs: dict = dict(timeout=20)
        if proxy:
            req_kwargs["proxies"] = {"http": proxy, "https": proxy}
        resp = scraper.get(url, **req_kwargs)
        if resp.status_code == 200:
            return resp.text
    except ImportError:
        pass
    except Exception as exc:
        logger.debug("cloudscraper %s: %s", url, exc)
    return None


# ══════════════════════════════════════════════════════════════════════════
#  7. سلسلة الـ Fallback الكاملة (مزامن — يُستدعى من executor)
# ══════════════════════════════════════════════════════════════════════════
def try_all_sync_fallbacks(url: str) -> Optional[str]:
    """
    يحاول بالترتيب:
    1. curl_cffi + proxy (TLS fingerprint)
    2. cloudscraper + proxy
    3. requests عادي + proxy
    كل محاولة تأخذ proxy جديد من الـ rotator.
    """
    pr = get_proxy_rotator()

    # المحاولة 1: curl_cffi
    proxy1 = pr.get_next()
    html = try_curl_cffi(url, proxy=proxy1)
    if html:
        if proxy1:
            pr.mark_success(proxy1)
        return html

    # المحاولة 2: cloudscraper
    proxy2 = pr.get_next()
    html = try_cloudscraper(url, proxy=proxy2)
    if html:
        if proxy2:
            pr.mark_success(proxy2)
        return html

    # المحاولة 3: requests بسيط
    try:
        import requests as _req
        headers = get_browser_headers(referer=f"https://{urlparse(url).netloc}/")
        proxy3 = pr.get_next()
        req_kwargs: dict = dict(headers=headers, timeout=20, allow_redirects=True, verify=False)
        if proxy3:
            req_kwargs["proxies"] = {"http": proxy3, "https": proxy3}
        resp = _req.get(url, **req_kwargs)
        if resp.status_code == 200:
            if proxy3:
                pr.mark_success(proxy3)
            return resp.text
        if proxy3 and resp.status_code in (403, 407):
            pr.mark_failed(proxy3)
    except Exception as exc:
        logger.debug("requests fallback %s: %s", url, exc)

    return None
