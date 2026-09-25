"""
Fetches all PSX intraday quotes in a single request via the /market-watch endpoint.

Returns a list of dicts — one per symbol — with fields:
  symbol, name, sector_code, indices,
  ldcp, open, high, low, price, change, change_pct, change_direction,
  volume,
  scraped_at  (UTC-aware datetime of the HTTP request)
"""

import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HOME_URL = "https://dps.psx.com.pk/"
MARKET_WATCH_URL = "https://dps.psx.com.pk/market-watch"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html, */*",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": HOME_URL,
}

_REQUEST_ID_RE = re.compile(r'window\.__ps\s*=\s*\{[^}]*"_k"\s*:\s*"([^"]+)"')


def _fetch_request_id(session):
    home = session.get(HOME_URL, headers={"Accept": "text/html, */*"}, timeout=15)
    home.raise_for_status()
    match = _REQUEST_ID_RE.search(home.text)
    if not match:
        raise RuntimeError("PSX homepage did not provide the AJAX request ID")
    return match.group(1)


def fetch_market_watch(session=None):
    """
    GET /market-watch and parse the HTML table into a list of quote dicts.

    Uses the data-order attribute on each <td> for clean numeric values —
    no string stripping or comma removal needed.

    Returns (quotes, scraped_at) where:
      quotes      — list of dicts, one per symbol row
      scraped_at  — UTC-aware datetime captured just before the HTTP request
    """
    sess = session or requests.Session()
    # PSX's browser first loads the homepage, then sends its per-page _k value
    # as X-Req-Id on same-origin AJAX requests. The token can expire while the
    # poller is running, so refresh it and retry once when PSX returns 403.
    sess.headers.update(HEADERS)
    if not sess.headers.get("X-Req-Id"):
        sess.headers["X-Req-Id"] = _fetch_request_id(sess)

    scraped_at = datetime.now(timezone.utc)
    resp = sess.get(MARKET_WATCH_URL, timeout=15)
    if resp.status_code == 403:
        logger.warning("market-watch request rejected (403); refreshing PSX request ID and retrying")
        sess.headers["X-Req-Id"] = _fetch_request_id(sess)
        scraped_at = datetime.now(timezone.utc)
        resp = sess.get(MARKET_WATCH_URL, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select("tbody.tbl__body tr")

    quotes = []
    for row in rows:
        tds = row.find_all("td")
        if len(tds) < 9:
            continue

        # td[0]: symbol + company name
        sym_td  = tds[0]
        symbol  = sym_td.get("data-order", "").strip()
        anchor  = sym_td.find("a")
        name    = anchor.get("data-title", "").strip() if anchor else None

        # td[1]: sector code (e.g. "0804")
        sector_code = tds[1].get_text(strip=True) or None

        # td[2]: comma-separated index memberships
        indices_raw = tds[2].get_text(strip=True)
        indices = [i.strip() for i in indices_raw.split(",") if i.strip()] if indices_raw else []

        # td[3..8]: all numeric values — read from data-order for precision
        def _f(td):
            v = td.get("data-order")
            try:
                return float(v) if v is not None else None
            except (ValueError, TypeError):
                return None

        ldcp       = _f(tds[3])
        open_      = _f(tds[4])
        high       = _f(tds[5])
        low        = _f(tds[6])
        price      = _f(tds[7])
        change     = _f(tds[8])
        change_pct = _f(tds[9])
        volume     = int(tds[10].get("data-order")) if tds[10].get("data-order") else None

        # Direction from CSS class on the change td
        change_cls       = tds[8].get("class", [])
        change_direction = "up" if "change__text--pos" in change_cls else "down"

        if not symbol:
            continue

        quotes.append({
            "symbol":           symbol,
            "name":             name,
            "sector_code":      sector_code,
            "indices":          indices,
            "ldcp":             ldcp,
            "open":             open_,
            "high":             high,
            "low":              low,
            "price":            price,
            "change":           change,
            "change_pct":       change_pct,
            "change_direction": change_direction,
            "volume":           volume,
            "scraped_at":       scraped_at,
        })

    logger.info("market-watch: parsed %d symbols in %.2fs", len(quotes),
                (datetime.now(timezone.utc) - scraped_at).total_seconds())
    return quotes, scraped_at


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    quotes, ts = fetch_market_watch()
    print(f"Fetched {len(quotes)} symbols at {ts.isoformat()}")
    print(json.dumps(quotes[:3], indent=2, default=str))
