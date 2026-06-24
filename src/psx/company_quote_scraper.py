"""
Scrapes the REG (regular) intraday quote from dps.psx.com.pk/company/<SYMBOL>.

Extracted fields (all from the REG tab):
  symbol, name, sector,
  price, change, change_pct,
  as_of (full timestamp),
  open, high, low, volume,
  circuit_breaker_low, circuit_breaker_high,
  day_range_low, day_range_high,
  ask_price, ask_volume, bid_price, bid_volume,
  ldcp, var, haircut, pe_ratio_ttm,
  scraped_at (UTC wall-clock of the HTTP request)
"""

import re
import logging
from datetime import datetime, timezone
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://dps.psx.com.pk/company/{symbol}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# "^ As of Wed, Jun 24, 2026 3:48 PM"  →  datetime
_AS_OF_RE = re.compile(
    r"As of\s+\w+,\s+(.+)", re.IGNORECASE
)
_AS_OF_FMT = "%b %d, %Y %I:%M %p"


def _clean(text: str) -> str:
    return text.strip().replace(" ", "").replace("–", "-").replace("—", "-")


def _float(text: str) -> Optional[float]:
    try:
        return float(re.sub(r"[^\d.\-]", "", _clean(text)))
    except (ValueError, TypeError):
        return None


def _int(text: str) -> Optional[int]:
    try:
        return int(re.sub(r"[^\d]", "", _clean(text)))
    except (ValueError, TypeError):
        return None


def _parse_as_of(raw: str) -> Optional[datetime]:
    """Parse 'As of Wed, Jun 24, 2026 3:48 PM' into a naive datetime (PKT)."""
    m = _AS_OF_RE.search(_clean(raw))
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1).strip(), _AS_OF_FMT)
    except ValueError:
        return None


def _parse_range(stats_value_el) -> tuple[Optional[float], Optional[float]]:
    """Extract low/high from a numRange data attribute or text like '411.72 – 503.22'."""
    nr = stats_value_el.find(class_="numRange")
    if nr:
        low = _float(nr.get("data-low", ""))
        high = _float(nr.get("data-high", ""))
        return low, high
    # Fallback: parse from visible text (strip child tags first)
    text = stats_value_el.get_text(separator=" ")
    parts = re.split(r"[–—\-–—]", text)
    if len(parts) == 2:
        return _float(parts[0]), _float(parts[1])
    return None, None


def _stats_map(panel) -> dict[str, str]:
    """Return {label: value_text} for all stats_item entries in a panel."""
    result = {}
    for item in panel.find_all(class_="stats_item"):
        label_el = item.find(class_="stats_label")
        value_el = item.find(class_="stats_value")
        if label_el and value_el:
            label = _clean(label_el.get_text())
            value = _clean(value_el.get_text(separator=" "))
            result[label] = value
    return result


def scrape_company_quote(symbol: str, session: Optional[requests.Session] = None) -> dict:
    """
    Fetch and parse the REG intraday quote for *symbol*.

    Returns a dict with all intraday fields plus metadata.
    Raises requests.RequestException on network failure.
    """
    sym = symbol.upper().strip()
    url = BASE_URL.format(symbol=sym)

    sess = session or requests.Session()
    scraped_at = datetime.now(timezone.utc)
    resp = sess.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    cq = soup.find(class_="company__quote")
    if not cq:
        raise ValueError(f"No company__quote section found for {sym}")

    # ── Identity ──────────────────────────────────────────────────────────────
    name_raw = _clean(cq.find(class_="quote__name").get_text()) if cq.find(class_="quote__name") else None
    name = re.sub(r"DELISTED$", "", name_raw).strip() if name_raw else None
    sector_el = cq.find(class_="quote__sector")
    sector = _clean(sector_el.find("span").get_text()) if sector_el and sector_el.find("span") else None

    # ── Price block ───────────────────────────────────────────────────────────
    close_el = cq.find(class_="quote__close")
    price_raw = _clean(close_el.get_text()) if close_el else ""
    price = _float(re.sub(r"^Rs\.", "", price_raw))

    change_el = cq.find(class_="quote__change")
    change = _float(cq.find(class_="change__value").get_text()) if cq.find(class_="change__value") else None
    change_pct_raw = _clean(cq.find(class_="change__percent").get_text()) if cq.find(class_="change__percent") else ""
    change_pct = _float(change_pct_raw)  # e.g. "(2.25%)" → 2.25
    change_direction = "up" if (change_el and "change__text--pos" in change_el.get("class", [])) else "down"

    # ── As-of timestamp ───────────────────────────────────────────────────────
    date_el = cq.find(class_="quote__date")
    as_of = _parse_as_of(date_el.get_text()) if date_el else None

    # ── REG tab stats ─────────────────────────────────────────────────────────
    reg_panel = cq.find(class_="tabs__panel", attrs={"data-name": "REG"})
    stats = _stats_map(reg_panel) if reg_panel else {}

    open_ = _float(stats.get("Open", ""))
    high = _float(stats.get("High", ""))
    low = _float(stats.get("Low", ""))
    volume = _int(stats.get("Volume", ""))
    ask_price = _float(stats.get("Ask Price", ""))
    ask_volume = _int(stats.get("Ask Volume", ""))
    bid_price = _float(stats.get("Bid Price", ""))
    bid_volume = _int(stats.get("Bid Volume", ""))
    ldcp = _float(stats.get("LDCP", ""))
    var_ = _float(stats.get("VAR", ""))
    haircut = _float(stats.get("HAIRCUT", ""))
    pe_ttm = _float(stats.get("P/E Ratio (TTM) **", ""))

    # Circuit breaker and day range come from numRange data attrs
    cb_low = cb_high = dr_low = dr_high = None
    if reg_panel:
        range_stats = reg_panel.find(class_="company__quote__rangeStats")
        if range_stats:
            items = range_stats.find_all(class_="stats_item")
            for item in items:
                label_el = item.find(class_="stats_label")
                value_el = item.find(class_="stats_value")
                if not label_el or not value_el:
                    continue
                label = _clean(label_el.get_text())
                if "CIRCUIT" in label.upper():
                    cb_low, cb_high = _parse_range(value_el)
                elif "DAY RANGE" in label.upper():
                    dr_low, dr_high = _parse_range(value_el)

    return {
        "symbol": sym,
        "name": name,
        "sector": sector,
        # price
        "price": price,
        "change": change,
        "change_pct": change_pct,
        "change_direction": change_direction,
        # timestamp (naive, Pakistan Standard Time)
        "as_of": as_of.isoformat() if as_of else None,
        # intraday OHLV
        "open": open_,
        "high": high,
        "low": low,
        "volume": volume,
        # circuit breaker
        "circuit_breaker_low": cb_low,
        "circuit_breaker_high": cb_high,
        # day range
        "day_range_low": dr_low,
        "day_range_high": dr_high,
        # order book
        "ask_price": ask_price,
        "ask_volume": ask_volume,
        "bid_price": bid_price,
        "bid_volume": bid_volume,
        # other stats
        "ldcp": ldcp,
        "var": var_,
        "haircut": haircut,
        "pe_ratio_ttm": pe_ttm,
        # metadata
        "scraped_at": scraped_at.isoformat(),
        "source_url": url,
    }


if __name__ == "__main__":
    import json
    import sys

    logging.basicConfig(level=logging.INFO)
    symbols = sys.argv[1:] or ["LUCK"]

    with requests.Session() as sess:
        for sym in symbols:
            try:
                quote = scrape_company_quote(sym, session=sess)
                print(json.dumps(quote, indent=2))
            except Exception as exc:
                logger.error("Failed for %s: %s", sym, exc)
