"""
scrapers/async_scraper.py — محرك الكشط غير المتزامن v3.0 (2026)
══════════════════════════════════════════════════════════════════
• يقرأ قائمة المتاجر من data/competitors_list.json
• يحدّد Sitemap لكل متجر عبر sitemap_resolve.py
• يستخرج بيانات المنتج بطبقات: JSON-LD → BeautifulSoup meta → regex fallback
• يكتب النتائج في data/competitors_latest.csv
• يحدّث data/scraper_progress.json لحظياً (للـ Dashboard)
• يدعم lastmod للكشط التزايدي (يكشط فقط الصفحات المحدّثة)
• ضد الحظر: Rotating Proxies + Smart Jitter + curl_cffi + cloudscraper

[v3 جديد]:
• كشط متجرين في نفس اللحظة بالتوازي (asyncio.gather)
• Checkpoint: يستأنف من نقطة التوقف عند الانقطاع (--resume)
• store_concurrency: عدد المتاجر المتوازية (افتراضي: 2)
• Retry Logic: 3 محاولات + Exponential Backoff لكل URL

التشغيل:
  python -m scrapers.async_scraper
  python -m scrapers.async_scraper --max-products 500 --concurrency 5
  python -m scrapers.async_scraper --resume                  # استئناف من Checkpoint
  python -m scrapers.async_scraper --store-concurrency 2    # متجرين بالتوازي
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout

from scrapers.anti_ban import (
    get_browser_headers,
    get_rate_limiter,
    fetch_with_retry,
    try_all_sync_fallbacks,
    get_proxy_rotator,
)

# ── Sprint 2 — DB Helpers (Graceful Degradation إذا DB غير جاهز) ──────────
try:
    from utils.db_manager import (
        insert_raw_staging,
        insert_scrape_hash,
        is_already_scraped_today,
        DB_PATH as PERFUME_DB,
    )
    _DB_AVAILABLE = True
except Exception:
    # إذا فشل الاستيراد → noop lambdas — الكاشط يعمل بدون DB
    insert_raw_staging       = lambda *a, **kw: ""        # type: ignore[assignment]
    insert_scrape_hash       = lambda *a, **kw: False     # type: ignore[assignment]
    is_already_scraped_today = lambda *a, **kw: False     # type: ignore[assignment]
    PERFUME_DB               = None
    _DB_AVAILABLE            = False

# ── مسارات ────────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
_DATA_DIR = (
    Path(os.environ.get("DATA_DIR", "")).resolve()
    if os.environ.get("DATA_DIR")
    else _ROOT / "data"
)
_DATA_DIR.mkdir(parents=True, exist_ok=True)

COMPETITORS_FILE  = _DATA_DIR / "competitors_list.json"
OUTPUT_CSV        = _DATA_DIR / "competitors_latest.csv"
PROGRESS_FILE     = _DATA_DIR / "scraper_progress.json"
LASTMOD_FILE      = _DATA_DIR / "scraper_lastmod_cache.json"
CHECKPOINT_FILE   = _DATA_DIR / "scraper_checkpoint.json"   # v3: استئناف من نقطة الانقطاع
ERROR_LOG         = _DATA_DIR / "scraper_errors.log"

CSV_COLS = ["store", "name", "price", "image", "url", "brand", "sku", "scraped_at"]

# ── إعداد logging ─────────────────────────────────────────────────────────
def _setup_logging() -> None:
    """يهيئ logging مرة واحدة فقط — StreamHandler وحده."""
    root = logging.getLogger()
    if root.handlers:
        return
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)
    root.setLevel(logging.INFO)


_setup_logging()
logger = logging.getLogger("scraper")

# ── Regexes للاستخراج ─────────────────────────────────────────────────────
_JSON_LD_RE   = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I,
)
_OG_TITLE_RE  = re.compile(
    r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_OG_PRICE_RE  = re.compile(
    r'<meta[^>]+property=["\']product:price:amount["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_OG_CURRENCY_RE = re.compile(
    r'<meta[^>]+property=["\']product:price:currency["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_OG_IMAGE_RE  = re.compile(
    r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']', re.I
)
_PRICE_META_RE = re.compile(
    r'<meta[^>]+(?:itemprop|property)=["\'](?:price|product:price:amount)["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)
_PRICE_META_CURRENCY_RE = re.compile(
    r'<meta[^>]+(?:itemprop|property)=["\'](?:priceCurrency|product:price:currency)["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)
_PRICE_CLASS_RE = re.compile(
    r'<[^>]+class=["\'][^"\']*(?:product-price|price|amount|text-sm-2)[^"\']*["\'][^>]*>(.*?)</[^>]+>',
    re.I | re.S,
)
_TAG_RE = re.compile(r"<[^>]+>")
import os as _os_scraper
_USD_TO_SAR = float(_os_scraper.environ.get("USD_TO_SAR", "3.75"))


# ══════════════════════════════════════════════════════════════════════════
#  Checkpoint — استئناف من نقطة الانقطاع (v3 جديد)
# ══════════════════════════════════════════════════════════════════════════
def _load_checkpoint() -> Set[str]:
    """
    يحمّل قائمة الـ URLs المكتملة من ملف الـ Checkpoint.
    يتجاهل الـ checkpoint إذا كان عمره أكثر من 24 ساعة.
    """
    try:
        data = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
        updated_at = data.get("updated_at", "2000-01-01T00:00:00")
        age_hours = (
            datetime.now() - datetime.fromisoformat(updated_at)
        ).total_seconds() / 3600
        if age_hours > 24:
            logger.info("Checkpoint منتهي الصلاحية (%.1f ساعة) — بدء من الصفر", age_hours)
            return set()
        done: Set[str] = set(data.get("completed_urls", []))
        logger.info(
            "Checkpoint مُحمَّل: %d URL مكتمل (عمره %.1f دقيقة)",
            len(done), age_hours * 60,
        )
        return done
    except Exception:
        return set()


def _save_checkpoint(done_urls: Set[str]) -> None:
    """يحفظ قائمة الـ URLs المكتملة — يُستدعى كل 100 URL."""
    try:
        CHECKPOINT_FILE.write_text(
            json.dumps(
                {
                    "completed_urls": list(done_urls),
                    "count": len(done_urls),
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
    except Exception as exc:
        logger.warning("فشل حفظ Checkpoint: %s", exc)


def _clear_checkpoint() -> None:
    """يحذف ملف الـ Checkpoint عند الانتهاء بنجاح."""
    try:
        CHECKPOINT_FILE.unlink(missing_ok=True)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════
#  CSV helpers — قراءة/دمج/كتابة مع حفظ البيانات القديمة
# ══════════════════════════════════════════════════════════════════════════
def _load_existing_csv() -> Dict[str, Dict[str, Any]]:
    """يقرأ CSV الموجود ويعيده كـ {url: row_dict} للدمج لاحقاً."""
    if not OUTPUT_CSV.exists():
        return {}
    try:
        rows: Dict[str, Dict[str, Any]] = {}
        with OUTPUT_CSV.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                u = (row.get("url") or "").strip()
                if u:
                    rows[u] = row
        return rows
    except Exception as exc:
        logger.warning("فشل قراءة CSV القديم: %s", exc)
        return {}


def _write_merged_csv(
    existing: Dict[str, Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
) -> int:
    """يدمج الصفوف القديمة مع الجديدة (الجديد يكسب عند تعارض الـ URL)،
    ثم يكتب الكل دفعة واحدة — يعيد عدد الصفوف النهائي."""
    merged = dict(existing)
    for row in new_rows:
        u = (row.get("url") or "").strip()
        if u:
            merged[u] = row
    if not merged:
        return 0
    tmp = OUTPUT_CSV.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLS, extrasaction="ignore")
        writer.writeheader()
        for row in merged.values():
            writer.writerow(row)
    tmp.replace(OUTPUT_CSV)  # atomic rename
    return len(merged)


# ══════════════════════════════════════════════════════════════════════════
#  Lastmod Cache — للكشط التزايدي
# ══════════════════════════════════════════════════════════════════════════
def _load_lastmod_cache() -> Dict[str, str]:
    try:
        return json.loads(LASTMOD_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_lastmod_cache(cache: Dict[str, str]) -> None:
    try:
        LASTMOD_FILE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as exc:
        logger.warning("فشل حفظ lastmod cache: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  Sprint 2 — _flush_db_to_csv: قراءة Staging بدُفعات → كتابة ذرية للـ CSV
# ══════════════════════════════════════════════════════════════════════════
def _flush_db_to_csv(
    existing_rows: Dict[str, Dict[str, Any]],
    chunk_size: int = 500,
) -> int:
    """
    يقرأ raw_scrape_staging (status='pending') على دُفعات (Chunks)
    ويدمجها مع الـ CSV الحالي بدون تحميل كل شيء في الذاكرة دفعة واحدة.
    يكتب CSV بشكل ذري (tmp → rename) لضمان سلامة الملف.
    يُحدّث status='processed' بعد الكتابة الناجحة.
    يُرجع إجمالي الصفوف في CSV بعد الدمج.
    """
    if not _DB_AVAILABLE or not PERFUME_DB:
        # Graceful Degradation — لا بيانات جديدة تُكتب
        logger.warning("_flush_db_to_csv: DB غير متاح — CSV لم يُحدَّث")
        return len(existing_rows)

    merged: Dict[str, Dict[str, Any]] = dict(existing_rows)
    processed_ids: List[int] = []

    # ── قراءة Staging على دُفعات ──────────────────────────────────────────
    try:
        import sqlite3 as _sq
        conn = _sq.connect(str(PERFUME_DB), check_same_thread=False, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.row_factory = _sq.Row
        offset = 0
        while True:
            rows = conn.execute(
                """SELECT id, competitor, url, name, price,
                          image_url, brand, sku, scraped_at
                   FROM raw_scrape_staging
                   WHERE status = 'pending'
                   ORDER BY id ASC
                   LIMIT ? OFFSET ?""",
                (chunk_size, offset),
            ).fetchall()
            if not rows:
                break
            for r in rows:
                url_key = (r["url"] or "").strip()
                if url_key:
                    merged[url_key] = {
                        "store":      r["competitor"] or "",
                        "name":       r["name"] or "",
                        "price":      r["price"] or "",
                        "image":      r["image_url"] or "",
                        "url":        url_key,
                        "brand":      r["brand"] or "",
                        "sku":        r["sku"] or "",
                        "scraped_at": (r["scraped_at"] or "")[:10],
                    }
                    processed_ids.append(int(r["id"]))
            offset += chunk_size
        conn.close()
    except Exception as exc:
        logger.warning("_flush_db_to_csv: فشل قراءة DB — %s", exc)

    if not merged:
        return 0

    # ── كتابة CSV ذرية: .tmp → rename ─────────────────────────────────────
    tmp = OUTPUT_CSV.with_suffix(".tmp")
    try:
        with tmp.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_COLS, extrasaction="ignore")
            writer.writeheader()
            for row in merged.values():
                writer.writerow(row)
        tmp.replace(OUTPUT_CSV)
    except Exception as exc:
        logger.warning("_flush_db_to_csv: فشل كتابة CSV — %s", exc)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return len(existing_rows)

    # ── تحديث status='processed' في DB بعد نجاح الكتابة ──────────────────
    if processed_ids:
        try:
            import sqlite3 as _sq2
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            placeholders = ",".join("?" * len(processed_ids))
            with _sq2.connect(str(PERFUME_DB), check_same_thread=False, timeout=30) as conn2:
                conn2.execute("PRAGMA journal_mode=WAL;")
                conn2.execute(
                    f"UPDATE raw_scrape_staging SET status='processed', processed_at=?"
                    f" WHERE id IN ({placeholders})",
                    [now_str, *processed_ids],
                )
                conn2.commit()
        except Exception as exc:
            logger.warning("_flush_db_to_csv: فشل تحديث status — %s", exc)

    logger.info(
        "_flush_db_to_csv: %d سجل جديد من DB + %d قديم → %d إجمالي في CSV",
        len(processed_ids), len(existing_rows), len(merged),
    )
    return len(merged)


# ══════════════════════════════════════════════════════════════════════════
#  Progress — يُحدِّث ملف JSON الحي
# ══════════════════════════════════════════════════════════════════════════
class Progress:
    def __init__(self, stores: List[str], total_urls: int):
        self._file = PROGRESS_FILE
        self._data: Dict[str, Any] = {
            "running":              True,
            "started_at":           datetime.now().isoformat(),
            "updated_at":           datetime.now().isoformat(),
            "stores_total":         len(stores),
            "stores_done":          0,
            "urls_total":           total_urls,
            "urls_processed":       0,
            "rows_in_csv":          0,
            "fetch_exceptions":     0,
            "success_rate_pct":     0,
            "current_store":        "",
            "current_stores":       [],     # v3: متاجر متوازية نشطة
            "last_error":           "",
            "output_file":          str(OUTPUT_CSV),
            "store_urls_total":     0,
            "store_urls_done":      0,
            "store_started_at":     "",
            "stores_results":       {},
            "stores_cached_counts": {},
            "stores_sitemap_failed":[],
        }
        self._flush()

    def update(self, **kwargs: Any) -> None:
        self._data.update(kwargs)
        self._data["updated_at"] = datetime.now().isoformat()
        if self._data["urls_processed"] > 0:
            ok = self._data["rows_in_csv"]
            tot = self._data["urls_processed"]
            self._data["success_rate_pct"] = round(ok / tot * 100, 1)
        self._flush()

    def done(self, rows: int) -> None:
        self._data.update({
            "running":        False,
            "rows_in_csv":    rows,
            "updated_at":     datetime.now().isoformat(),
            "finished_at":    datetime.now().isoformat(),
            "current_store":  "",
            "current_stores": [],
        })
        self._flush()

    def error(self, msg: str) -> None:
        self._data["last_error"] = msg[:500]
        self._data["fetch_exceptions"] = self._data.get("fetch_exceptions", 0) + 1
        self._flush()

    def _flush(self) -> None:
        try:
            PROGRESS_FILE.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("progress flush failed: %s", exc)


# ══════════════════════════════════════════════════════════════════════════
#  استخراج بيانات المنتج من HTML
# ══════════════════════════════════════════════════════════════════════════
def _parse_price(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if float(raw) > 0 else None
    s0 = str(raw).strip().translate(str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789'))
    s0 = s0.replace("٬", ",").replace("،", ",")
    s = re.sub(r"[^\d.,]", "", s0)
    if not s:
        return None
    dot_idx   = s.rfind('.')
    comma_idx = s.rfind(',')
    if dot_idx >= 0 and comma_idx >= 0:
        if comma_idx > dot_idx:
            s = s.replace('.', '').replace(',', '.')
        else:
            s = s.replace(',', '')
    elif comma_idx >= 0:
        parts = s.split(',')
        if len(parts) == 2 and len(parts[-1]) == 3:
            s = s.replace(',', '')
        else:
            s = s.replace(',', '.')
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _strip_tags(text: str) -> str:
    return _TAG_RE.sub(" ", str(text or "")).strip()


def _currency_hint(text: str) -> str:
    t = str(text or "").upper()
    if not t:
        return ""
    if any(x in t for x in ("SAR", "ر.س", "﷼", "ريال")):
        return "SAR"
    if "USD" in t or "$" in t:
        return "USD"
    return ""


def _price_to_sar(price: Optional[float], currency: str = "", raw_text: str = "") -> Optional[float]:
    if price is None or price <= 0:
        return None
    c = str(currency or "").strip().upper()
    if not c:
        c = _currency_hint(raw_text)
    if c == "USD":
        return round(float(price) * _USD_TO_SAR, 2)
    return float(price)


def _pick_price_candidate(candidates: List[tuple]) -> Optional[float]:
    if not candidates:
        return None
    for p, cur, raw in candidates:
        if str(cur or "").strip().upper() == "SAR":
            v = _price_to_sar(p, cur, raw)
            if v and v > 0:
                return v
    for p, cur, raw in candidates:
        v = _price_to_sar(p, cur, raw)
        if v and v > 0:
            return v
    return None


def _extract_price_from_common_classes(html: str) -> Optional[tuple]:
    for m in _PRICE_CLASS_RE.finditer(html or ""):
        raw = _strip_tags(m.group(1))
        p = _parse_price(raw)
        if p and p > 0:
            return p, _currency_hint(raw), raw
    return None


def _extract_from_jsonld(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    for block in _JSON_LD_RE.findall(html):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        node = _find_product_node(data)
        if node is None:
            continue
        name = node.get("name")
        if isinstance(name, dict):
            name = name.get("value") or name.get("text") or name.get("@value") or str(name)
        name = str(name or "").strip()
        if not name:
            continue
        price_candidates: List[tuple] = []
        offers = node.get("offers") or node.get("Offers") or {}
        offers_list = offers if isinstance(offers, list) else [offers]
        for off in offers_list:
            if isinstance(off, dict):
                otype = str(off.get("@type", "") or "")
                if otype == "AggregateOffer":
                    price_raw = off.get("lowPrice") or off.get("highPrice") or off.get("price")
                else:
                    price_raw = off.get("price")
                cur = off.get("priceCurrency") or off.get("currency") or node.get("priceCurrency") or ""
            else:
                price_raw = off
                cur = node.get("priceCurrency") or ""
            p = _parse_price(price_raw)
            if p and p > 0:
                price_candidates.append((p, str(cur or ""), str(price_raw or "")))
        if not price_candidates:
            p = _parse_price(node.get("price"))
            if p and p > 0:
                price_candidates.append((p, str(node.get("priceCurrency") or ""), str(node.get("price") or "")))
        price = _pick_price_candidate(price_candidates)
        if price is None or price <= 0:
            continue
        img_field = node.get("image", "")
        if isinstance(img_field, list):
            image = str(img_field[0]).strip() if img_field else ""
        elif isinstance(img_field, dict):
            image = str(img_field.get("url") or img_field.get("contentUrl") or "").strip()
        else:
            image = str(img_field or "").strip()
        brand_field = node.get("brand") or {}
        if isinstance(brand_field, dict):
            brand = str(brand_field.get("name") or brand_field.get("@value") or "").strip()
        elif isinstance(brand_field, list) and brand_field:
            b0 = brand_field[0]
            brand = str(b0.get("name") if isinstance(b0, dict) else b0).strip()
        else:
            brand = str(brand_field or "").strip()
        url_out = str(node.get("url") or node.get("@id") or page_url).strip()
        sku = str(node.get("sku") or node.get("productID") or node.get("mpn") or "").strip()
        return {
            "name": name, "price": price, "image": image,
            "url": url_out or page_url, "brand": brand, "sku": sku,
        }
    return None


def _find_product_node(obj: Any) -> Optional[dict]:
    if isinstance(obj, list):
        for item in obj:
            found = _find_product_node(item)
            if found is not None:
                return found
        return None
    if isinstance(obj, dict):
        t = obj.get("@type")
        types = t if isinstance(t, list) else ([t] if t else [])
        if "Product" in types or "ProductGroup" in types:
            if "ProductGroup" in types and "Product" not in types:
                hv = obj.get("hasVariant")
                if isinstance(hv, list) and hv and isinstance(hv[0], dict):
                    return hv[0]
            return obj
        if "@graph" in obj:
            found = _find_product_node(obj["@graph"])
            if found is not None:
                return found
        for v in obj.values():
            if isinstance(v, (dict, list)):
                found = _find_product_node(v)
                if found is not None:
                    return found
    return None


def _extract_from_og(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    name_m = _OG_TITLE_RE.search(html)
    price_m = _OG_PRICE_RE.search(html) or _PRICE_META_RE.search(html)
    image_m = _OG_IMAGE_RE.search(html)
    if not name_m or not price_m:
        return None
    cur_m = _OG_CURRENCY_RE.search(html) or _PRICE_META_CURRENCY_RE.search(html)
    cur = cur_m.group(1).strip() if cur_m else ""
    price = _price_to_sar(_parse_price(price_m.group(1)), cur, price_m.group(1))
    if price is None:
        return None
    return {
        "name":  name_m.group(1).strip(),
        "price": price,
        "image": image_m.group(1) if image_m else "",
        "url":   page_url,
        "brand": "",
        "sku":   "",
    }


def _extract_from_html_patterns(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    candidates: List[tuple] = []
    for pm in re.finditer(r'"price"\s*:\s*"?([\d.,]+)"?', html or "", re.I):
        raw = pm.group(1)
        around = (html or "")[max(0, pm.start() - 220): pm.end() + 220]
        cm = re.search(r'"priceCurrency"\s*:\s*"([A-Za-z]{3})"', around, re.I)
        cur = cm.group(1).strip() if cm else _currency_hint(around)
        p = _parse_price(raw)
        if p and p > 0:
            candidates.append((p, cur, raw))
    meta_price_m = _OG_PRICE_RE.search(html or "") or _PRICE_META_RE.search(html or "")
    if meta_price_m:
        cur_m = _OG_CURRENCY_RE.search(html or "") or _PRICE_META_CURRENCY_RE.search(html or "")
        cur = cur_m.group(1).strip() if cur_m else _currency_hint(meta_price_m.group(1))
        p = _parse_price(meta_price_m.group(1))
        if p and p > 0:
            candidates.append((p, cur, meta_price_m.group(1)))
    class_hit = _extract_price_from_common_classes(html or "")
    if class_hit:
        candidates.append(class_hit)
    price = _pick_price_candidate(candidates)
    if price is None or price <= 0:
        return None
    name_m = _OG_TITLE_RE.search(html)
    if not name_m:
        title_m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
        if not title_m:
            return None
        name = title_m.group(1).strip()
    else:
        name = name_m.group(1).strip()
    if not name:
        return None
    image_m = _OG_IMAGE_RE.search(html)
    return {
        "name": name, "price": price,
        "image": image_m.group(1) if image_m else "",
        "url": page_url, "brand": "", "sku": "",
    }


_DESC_META_RE = re.compile(
    r'<meta\s+(?:name|property)=["\'](?:description|og:description)["\'][^>]*content=["\']([^"\']{10,})["\']',
    re.I,
)
_DESC_CONTENT_RE = re.compile(
    r'<meta\s+content=["\']([^"\']{10,})["\'][^>]*(?:name|property)=["\'](?:description|og:description)["\']',
    re.I,
)
_DESC_DIV_RE = re.compile(
    r'<(?:div|section|p)[^>]+class=["\'][^"\']*(?:description|details|product-info|product-body)[^"\']*["\'][^>]*>'
    r'([\s\S]{30,1200}?)</(?:div|section|p)>',
    re.I,
)
_HTML_TAG_RE = re.compile(r'<[^>]+>')


def _extract_raw_description(html: str) -> str:
    if not html:
        return ""
    for pattern in (_DESC_META_RE, _DESC_CONTENT_RE):
        m = pattern.search(html)
        if m:
            txt = m.group(1).strip()
            if len(txt) >= 20:
                return txt[:1500]
    m2 = _DESC_DIV_RE.search(html)
    if m2:
        raw = _HTML_TAG_RE.sub(" ", m2.group(1))
        raw = re.sub(r'\s+', ' ', raw).strip()
        if len(raw) >= 30:
            return raw[:1500]
    return ""


def extract_product(html: str, page_url: str) -> Optional[Dict[str, Any]]:
    """يستخرج بيانات المنتج بثلاث طبقات: JSON-LD → OG → regex."""
    result = (
        _extract_from_jsonld(html, page_url)
        or _extract_from_og(html, page_url)
        or _extract_from_html_patterns(html, page_url)
    )
    if result is not None:
        result.setdefault("raw_description", _extract_raw_description(html))
    return result


# ══════════════════════════════════════════════════════════════════════════
#  جلب صفحة منتج واحدة — retry + anti-ban + fallbacks
# ══════════════════════════════════════════════════════════════════════════
_DOMAIN_MAX_FAIL = 25


async def fetch_product(
    session: aiohttp.ClientSession,
    url: str,
    store_domain: str,
    sem: asyncio.Semaphore,
    progress: Progress,
    cb: Dict[str, int],
) -> Optional[Dict[str, Any]]:
    if cb.get(store_domain, 0) >= _DOMAIN_MAX_FAIL:
        return None

    async with sem:
        resp = await fetch_with_retry(
            session, url, max_retries=3, referer=f"https://{store_domain}/"
        )

        html: Optional[str] = None
        if resp is not None:
            try:
                html = await resp.text(errors="ignore")
            except Exception:
                html = None

        if not html:
            loop = asyncio.get_running_loop()
            html = await loop.run_in_executor(None, try_all_sync_fallbacks, url)

        progress.update(urls_processed=progress._data.get("urls_processed", 0) + 1)

        if not html:
            cb[store_domain] = cb.get(store_domain, 0) + 1
            if cb[store_domain] == _DOMAIN_MAX_FAIL:
                logger.warning(
                    "Circuit Breaker: %s — %d فشل متتالٍ، تخطّي الروابط المتبقية.",
                    store_domain, _DOMAIN_MAX_FAIL,
                )
            return None

        cb[store_domain] = 0
        product = extract_product(html, url)
        if product:
            product["store"] = store_domain
            product["scraped_at"] = datetime.now().strftime("%Y-%m-%d")
        return product


# ══════════════════════════════════════════════════════════════════════════
#  كشط متجر واحد — coroutine مستقلة للتشغيل المتوازي (v3 جديد)
# ══════════════════════════════════════════════════════════════════════════
async def _scrape_single_store(
    store_url: str,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    progress: Progress,
    circuit_breaker: Dict[str, int],
    existing_rows: Dict[str, Any],       # محجوز للتوافق — Sprint 2: يُقرأ في _flush_db_to_csv
    lastmod_cache: Dict[str, str],
    new_lastmod_cache: Dict[str, str],
    checkpoint_urls: Set[str],
    max_products_per_store: int,
    incremental: bool,
    existing_by_store: Any,
) -> None:
    """
    يكشط متجراً واحداً كاملاً.
    Sprint 2: يكتب النتائج مباشرة في raw_scrape_staging بدلاً من قائمة في الذاكرة.
    مصمَّم للتشغيل المتوازي مع متاجر أخرى عبر asyncio.gather.
    """
    from scrapers.sitemap_resolve import resolve_product_entries

    domain = urlparse(store_url).netloc

    # تحديث قائمة المتاجر النشطة في Progress
    _active = list(progress._data.get("current_stores") or [])
    if domain not in _active:
        _active.append(domain)
    progress.update(
        current_store=domain,
        current_stores=_active,
        store_urls_total=0,
        store_urls_done=0,
        store_started_at=datetime.now().isoformat(),
    )
    logger.info("→ بدء كشط: %s", domain)

    # حل الـ Sitemap
    try:
        entries = await resolve_product_entries(
            store_url, session, max_products=max_products_per_store
        )
    except Exception as exc:
        progress.error(f"resolve {domain}: {exc}")
        entries = []

    if not entries:
        logger.warning("لا روابط منتجات لـ %s (فشل Sitemap)", domain)
        _res = dict(progress._data.get("stores_results") or {})
        _res[domain] = 0
        _failed = list(progress._data.get("stores_sitemap_failed") or [])
        if domain not in _failed:
            _failed.append(domain)
        _active2 = [s for s in (progress._data.get("current_stores") or []) if s != domain]
        progress.update(
            stores_done=progress._data["stores_done"] + 1,
            stores_results=_res,
            stores_sitemap_failed=_failed,
            current_stores=_active2,
        )
        return

    # فلترة تزايدية: تخطي lastmod غير المتغير
    urls_to_fetch: List[str] = []
    for entry in entries:
        if incremental and entry.lastmod:
            cached = lastmod_cache.get(entry.url, "")
            if cached == entry.lastmod:
                continue
            new_lastmod_cache[entry.url] = entry.lastmod
        urls_to_fetch.append(entry.url)

    if not urls_to_fetch and entries:
        _cached_n = existing_by_store.get(domain, 0)
        logger.info(
            "%s — %d منتج بلا تحديث (lastmod لم يتغير) → تخطّى، محفوظ مسبقاً: %d",
            domain, len(entries), _cached_n,
        )
        _res = dict(progress._data.get("stores_results") or {})
        _res[domain] = -_cached_n
        _active2 = [s for s in (progress._data.get("current_stores") or []) if s != domain]
        progress.update(
            stores_done=progress._data["stores_done"] + 1,
            stores_results=_res,
            current_stores=_active2,
        )
        return

    # تطبيق Checkpoint: تخطّي الـ URLs المكتملة في جلسة سابقة
    checkpoint_skipped = 0
    if checkpoint_urls:
        before = len(urls_to_fetch)
        urls_to_fetch = [u for u in urls_to_fetch if u not in checkpoint_urls]
        checkpoint_skipped = before - len(urls_to_fetch)
        if checkpoint_skipped:
            logger.info(
                "%s — تخطّى %d URL (Checkpoint من جلسة سابقة)، يبقى: %d",
                domain, checkpoint_skipped, len(urls_to_fetch),
            )

    if not urls_to_fetch:
        logger.info("%s — جميع الـ URLs مكتملة من Checkpoint", domain)
        _active2 = [s for s in (progress._data.get("current_stores") or []) if s != domain]
        progress.update(
            stores_done=progress._data["stores_done"] + 1,
            current_stores=_active2,
        )
        return

    # Sprint 2: فلتر DB Dedup — تخطّي الـ URLs المكشوطة اليوم مسبقاً في scrape_hashes
    if _DB_AVAILABLE:
        _before_db = len(urls_to_fetch)
        urls_to_fetch = [
            u for u in urls_to_fetch
            if not is_already_scraped_today(u, competitor=domain, db_path=PERFUME_DB)
        ]
        _db_skipped = _before_db - len(urls_to_fetch)
        if _db_skipped:
            logger.info(
                "%s — تخطّى %d URL (DB Dedup — مكشوط اليوم)، يبقى: %d",
                domain, _db_skipped, len(urls_to_fetch),
            )

    if not urls_to_fetch:
        logger.info("%s — جميع الـ URLs مكشوطة اليوم (DB Dedup)", domain)
        _active2 = [s for s in (progress._data.get("current_stores") or []) if s != domain]
        progress.update(
            stores_done=progress._data["stores_done"] + 1,
            current_stores=_active2,
        )
        return

    logger.info(
        "%s — %d للكشط (من %d في Sitemap، تخطّى Checkpoint: %d)",
        domain, len(urls_to_fetch), len(entries), checkpoint_skipped,
    )
    progress.update(store_urls_total=len(urls_to_fetch), store_urls_done=0)

    tasks = [
        fetch_product(session, url, domain, sem, progress, circuit_breaker)
        for url in urls_to_fetch
    ]

    _store_rows = 0

    # Sprint 2: gather بدلاً من as_completed — نتائج مرتبة مع url، تفريغ ذاكرة بعد المعالجة
    _gather_results = await asyncio.gather(*tasks, return_exceptions=True)

    for _idx, (url_fetched, result) in enumerate(zip(urls_to_fetch, _gather_results)):
        progress.update(store_urls_done=_idx + 1)

        # تحديث Checkpoint بعد كل 100 URL
        checkpoint_urls.add(url_fetched)
        if (_idx + 1) % 100 == 0:
            _save_checkpoint(checkpoint_urls)

        if isinstance(result, Exception):
            progress.error(f"{domain}: {result}")
            continue

        if result:
            # Sprint 2: كتابة مباشرة في DB — صفر تراكم في الذاكرة
            if _DB_AVAILABLE:
                insert_raw_staging(
                    competitor=domain,
                    url=url_fetched,
                    name=result.get("name", ""),
                    price=float(result.get("price") or 0),
                    image_url=result.get("image", ""),
                    brand=result.get("brand", ""),
                    sku=result.get("sku", ""),
                    raw_data={k: v for k, v in result.items() if k != "store"},
                    db_path=PERFUME_DB,
                )
                insert_scrape_hash(url=url_fetched, competitor=domain, db_path=PERFUME_DB)
            _store_rows += 1

    # حفظ نقطة تحقق نهائية للمتجر
    _save_checkpoint(checkpoint_urls)

    # تحديث نهائي لهذا المتجر (rows_in_csv يُحسَب في _flush_db_to_csv بنهاية run_scraper)
    _res = dict(progress._data.get("stores_results") or {})
    _res[domain] = _store_rows
    _active2 = [s for s in (progress._data.get("current_stores") or []) if s != domain]
    progress.update(
        stores_done=progress._data["stores_done"] + 1,
        stores_results=_res,
        store_urls_done=0,
        store_urls_total=0,
        current_stores=_active2,
    )
    logger.info("  ✓ %s — %d منتج كُتب في DB", domain, _store_rows)


# ══════════════════════════════════════════════════════════════════════════
#  المحرك الرئيسي — كشط متوازٍ لمجموعات من المتاجر (v3)
# ══════════════════════════════════════════════════════════════════════════
async def run_scraper(
    max_products_per_store: int = 0,
    concurrency: int = 3,
    incremental: bool = True,
    store_concurrency: int = 2,
    resume: bool = False,
) -> int:
    """
    يشغّل الكشط الكامل ويُرجع عدد الصفوف المكتوبة.

    store_concurrency: عدد المتاجر التي تُكشط في نفس الوقت (الافتراضي: 2).
    resume=True: يستأنف من Checkpoint إذا توقف الكاشط سابقاً.
    max_products_per_store=0: كشط جميع المنتجات بدون سقف.
    incremental=True: يتخطى الصفحات التي لم يتغير lastmod.
    """
    if not COMPETITORS_FILE.exists():
        logger.error("ملف المنافسين غير موجود: %s", COMPETITORS_FILE)
        return 0

    stores: List[str] = json.loads(COMPETITORS_FILE.read_text(encoding="utf-8"))
    if not stores:
        logger.error("قائمة المتاجر فارغة")
        return 0

    pr = get_proxy_rotator()
    proxy_status = f"{pr.active_count}/{len(pr._proxies)} proxy" if pr.has_proxies else "بدون proxy"

    logger.info(
        "بدء الكشط v3 — %d متجر، %d متوازٍ، تزامن %d، تزايدي=%s، resume=%s، %s",
        len(stores), store_concurrency, concurrency, incremental, resume, proxy_status,
    )

    # تحميل Checkpoint إذا طُلب الاستئناف
    checkpoint_urls: Set[str] = _load_checkpoint() if resume else set()
    if resume and checkpoint_urls:
        logger.info("وضع الاستئناف: سيُتخطى %d URL مكتمل من جلسة سابقة", len(checkpoint_urls))

    lastmod_cache = _load_lastmod_cache() if incremental else {}
    existing_rows = _load_existing_csv()
    logger.info("سجلات قديمة في CSV: %d", len(existing_rows))

    progress = Progress(stores, total_urls=len(stores) * max(max_products_per_store, 500))

    from collections import Counter as _Counter
    _existing_by_store = _Counter(
        r.get("store", "").strip() for r in existing_rows.values() if r.get("store")
    )
    progress.update(stores_cached_counts=dict(_existing_by_store))

    sem = asyncio.Semaphore(concurrency)
    new_lastmod_cache: Dict[str, str] = {}
    circuit_breaker: Dict[str, int] = {}

    connector = aiohttp.TCPConnector(ssl=False, limit=concurrency + 4)
    timeout = ClientTimeout(total=30, connect=10)
    default_headers = get_browser_headers()

    async with aiohttp.ClientSession(
        connector=connector, timeout=timeout, headers=default_headers,
        cookies={'currency': 'SAR'},
    ) as session:
        # معالجة المتاجر في دُفعات متوازية بحجم store_concurrency
        for batch_start in range(0, len(stores), store_concurrency):
            batch = stores[batch_start: batch_start + store_concurrency]
            batch_labels = [urlparse(s).netloc for s in batch]
            logger.info(
                "دُفعة متاجر [%d/%d]: %s",
                batch_start // store_concurrency + 1,
                (len(stores) + store_concurrency - 1) // store_concurrency,
                " + ".join(batch_labels),
            )

            # تشغيل المتاجر في الدُفعة بالتوازي الكامل
            coros = [
                _scrape_single_store(
                    store_url=s,
                    session=session,
                    sem=sem,
                    progress=progress,
                    circuit_breaker=circuit_breaker,
                    existing_rows=existing_rows,
                    lastmod_cache=lastmod_cache,
                    new_lastmod_cache=new_lastmod_cache,
                    checkpoint_urls=checkpoint_urls,
                    max_products_per_store=max_products_per_store,
                    incremental=incremental,
                    existing_by_store=_existing_by_store,
                )
                for s in batch
            ]
            results = await asyncio.gather(*coros, return_exceptions=True)

            # تسجيل أي استثناءات من الكاشط المتوازي
            for store_url, res in zip(batch, results):
                if isinstance(res, Exception):
                    domain = urlparse(store_url).netloc
                    logger.error("استثناء غير معالَج في %s: %s", domain, res)
                    progress.error(f"{domain}: {res}")

    # Sprint 2: قراءة من raw_scrape_staging بدُفعات (Chunks) → كتابة ذرية للـ CSV
    total_csv = _flush_db_to_csv(existing_rows)

    if incremental:
        _save_lastmod_cache({**lastmod_cache, **new_lastmod_cache})

    # حذف Checkpoint عند الانتهاء بنجاح
    _clear_checkpoint()

    progress.done(total_csv)
    logger.info("اكتمل الكشط — %d إجمالي في %s", total_csv, OUTPUT_CSV)
    return total_csv


# ══════════════════════════════════════════════════════════════════════════
#  نقطة الدخول عند التشغيل المباشر
# ══════════════════════════════════════════════════════════════════════════
def main() -> None:
    parser = argparse.ArgumentParser(description="Async Competitor Scraper v3.0")
    parser.add_argument(
        "--max-products", type=int, default=0,
        help="أقصى عدد منتجات لكل متجر (0 = جميع المنتجات بلا سقف)",
    )
    parser.add_argument(
        "--concurrency", type=int, default=3,
        help="عدد الطلبات المتزامنة لكل متجر (الافتراضي 3)",
    )
    parser.add_argument(
        "--store-concurrency", type=int, default=2,
        help="عدد المتاجر التي تُكشط في نفس الوقت (الافتراضي 2)",
    )
    parser.add_argument(
        "--full", action="store_true",
        help="كشط كامل (يتخطى الـ lastmod cache)",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="استئناف من Checkpoint — يتخطى الـ URLs المكتملة من جلسة سابقة",
    )
    args = parser.parse_args()

    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        rows = asyncio.run(
            run_scraper(
                max_products_per_store=args.max_products,
                concurrency=args.concurrency,
                incremental=not args.full,
                store_concurrency=args.store_concurrency,
                resume=args.resume,
            )
        )
        sys.exit(0 if rows > 0 else 1)
    except KeyboardInterrupt:
        try:
            prog = json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
            prog["running"] = False
            prog["last_error"] = "أُوقف يدوياً"
            PROGRESS_FILE.write_text(
                json.dumps(prog, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        logger.info("الكشط أُوقف يدوياً — الـ Checkpoint محفوظ للاستئناف لاحقاً")
        sys.exit(0)
    except Exception as exc:
        logger.error("خطأ فادح: %s", exc)
        try:
            err_state = {
                "running": False, "last_error": str(exc)[:500],
                "updated_at": datetime.now().isoformat(),
            }
            PROGRESS_FILE.write_text(
                json.dumps(err_state, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
