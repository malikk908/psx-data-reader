"""
Continuously-running intraday quote poller for PSX stocks.

Fetches live quotes from dps.psx.com.pk/company/<SYMBOL> for a slice of
symbols during PSX market hours, storing each unique snapshot in MongoDB.
Sleeps automatically when the market is closed (overnight and weekends).

Multi-instance usage — split ~600 symbols across 5 workers:
    python intraday_poller.py --start 1   --end 120
    python intraday_poller.py --start 121 --end 240
    python intraday_poller.py --start 241 --end 360
    python intraday_poller.py --start 361 --end 480
    python intraday_poller.py --start 481
"""

import argparse
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from datetime import time as time_type

import pymongo
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError, PyMongoError

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from psx.company_quote_scraper import scrape_company_quote
from psx.mongodb_cron import get_stock_symbols_range

logger = logging.getLogger(__name__)

# Pakistan Standard Time — UTC+5, no DST
PKT = timezone(timedelta(hours=5))
MARKET_OPEN  = time_type(9, 25)   # 5-min warmup before 09:30 official open
MARKET_CLOSE = time_type(15, 30)

INTRADAY_COLLECTION = "intraday_klines_temp"


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def test_mongo_connectivity(connection_string, db_name):
    """
    Perform a quick connectivity test to MongoDB.

    Attempts to connect and run a simple ping command. Returns True if
    successful, False otherwise.
    """
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        _ = client[db_name].name
        print(f"MongoDB connectivity OK for {connection_string} (db: {db_name})")
        return True
    except Exception as e:
        print(f"MongoDB connectivity check FAILED: {e}")
        return False
    finally:
        try:
            if 'client' in locals():
                client.close()
        except Exception:
            pass


def ensure_indexes(collection):
    """
    Create indexes on intraday_klines_temp. Idempotent — safe to call every startup.

    1. Unique compound on (symbol, as_of) — the upsert filter key.
    2. TTL on scraped_at (UTC datetime) — auto-purge docs after 48 hours.
    """
    collection.create_index(
        [("symbol", pymongo.ASCENDING), ("as_of", pymongo.ASCENDING)],
        unique=True,
        name="symbol_as_of_unique",
    )
    # 48 h = 172800 s
    collection.create_index(
        [("scraped_at", pymongo.ASCENDING)],
        expireAfterSeconds=172800,
        name="scraped_at_ttl",
    )
    logger.info("Indexes ensured on %s.", INTRADAY_COLLECTION)


# ---------------------------------------------------------------------------
# Market-hours logic
# ---------------------------------------------------------------------------

def now_pkt():
    """Return current datetime in PKT (UTC+5)."""
    return datetime.now(timezone.utc).astimezone(PKT)


def seconds_until_next_open():
    """
    Return (seconds_float, reason_str).

    Returns (0, "open") when the market is currently in session.
    Otherwise returns the seconds until 09:25 PKT on the next trading day.

    Note: no PSX public holiday calendar — on ad-hoc holidays the scraper
    will fetch stale quotes, which the upsert key handles gracefully.
    """
    now = now_pkt()
    wd  = now.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
    t   = now.time().replace(second=0, microsecond=0)

    # Market is open right now
    if wd <= 4 and MARKET_OPEN <= t < MARKET_CLOSE:
        return (0.0, "open")

    # Determine next opening datetime in PKT
    if wd <= 4 and t < MARKET_OPEN:
        # Today, pre-open
        next_open = now.replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd <= 3 and t >= MARKET_CLOSE:
        # Mon–Thu after close → tomorrow
        next_open = (now + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd == 4 and t >= MARKET_CLOSE:
        # Friday after close → Monday
        next_open = (now + timedelta(days=3)).replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd == 5:
        # Saturday → Monday
        next_open = (now + timedelta(days=2)).replace(hour=9, minute=25, second=0, microsecond=0)
    else:
        # Sunday → Monday
        next_open = (now + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)

    seconds = (next_open - now).total_seconds()
    reason  = f"next open {next_open.strftime('%a %Y-%m-%d %H:%M')} PKT"
    return (max(seconds, 0.0), reason)


def sleep_until_open():
    """
    Block until the market is open, sleeping in 60-second chunks so that
    KeyboardInterrupt is handled quickly. Logs once at entry and once on wake.
    """
    seconds, reason = seconds_until_next_open()
    if seconds <= 0:
        return

    target_utc = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    logger.info(
        "Market closed (%s). Sleeping %.0f min until ~%s PKT.",
        reason,
        seconds / 60,
        target_utc.astimezone(PKT).strftime("%Y-%m-%d %H:%M"),
    )

    while True:
        remaining = (target_utc - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 60.0))

    logger.info("Waking up — market should be open now.")


# ---------------------------------------------------------------------------
# Document building
# ---------------------------------------------------------------------------

def build_upsert_doc(quote):
    """
    Convert a raw quote dict from scrape_company_quote() into a MongoDB
    UpdateOne operation keyed on (symbol, as_of).

    as_of  → stored as naive datetime (PKT) — keeps PSX timezone convention.
    scraped_at → stored as UTC-aware datetime — required by MongoDB TTL index.
    """
    as_of_dt = datetime.fromisoformat(quote["as_of"]) if quote.get("as_of") else None
    # scraped_at comes as an aware ISO string from the scraper; re-attach tzinfo
    scraped_at_dt = datetime.fromisoformat(quote["scraped_at"])
    if scraped_at_dt.tzinfo is None:
        scraped_at_dt = scraped_at_dt.replace(tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc)

    set_fields = {
        "price":                quote.get("price"),
        "change":               quote.get("change"),
        "change_pct":           quote.get("change_pct"),
        "change_direction":     quote.get("change_direction"),
        "open":                 quote.get("open"),
        "high":                 quote.get("high"),
        "low":                  quote.get("low"),
        "volume":               quote.get("volume"),
        "circuit_breaker_low":  quote.get("circuit_breaker_low"),
        "circuit_breaker_high": quote.get("circuit_breaker_high"),
        "day_range_low":        quote.get("day_range_low"),
        "day_range_high":       quote.get("day_range_high"),
        "ask_price":            quote.get("ask_price"),
        "ask_volume":           quote.get("ask_volume"),
        "bid_price":            quote.get("bid_price"),
        "bid_volume":           quote.get("bid_volume"),
        "ldcp":                 quote.get("ldcp"),
        "var":                  quote.get("var"),
        "haircut":              quote.get("haircut"),
        "pe_ratio_ttm":         quote.get("pe_ratio_ttm"),
        "name":                 quote.get("name"),
        "sector":               quote.get("sector"),
        "source_url":           quote.get("source_url"),
        "scraped_at":           scraped_at_dt,
        "updated_at":           now_utc,
    }

    return pymongo.UpdateOne(
        {"symbol": quote["symbol"], "as_of": as_of_dt},
        {
            "$set":         set_fields,
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------

def run_poll_cycle(symbols, collection, symbol_delay):
    """
    Fetch a fresh quote for every symbol once, then bulk-upsert all results.

    Uses a single requests.Session for the whole cycle.
    Skips failed symbols (logs the error) without crashing.
    Returns a summary dict.
    """
    ops           = []
    success_count = 0
    error_count   = 0
    cycle_start   = time.monotonic()

    with requests.Session() as sess:
        for i, symbol in enumerate(symbols):
            sym_start = time.monotonic()
            try:
                quote   = scrape_company_quote(symbol, session=sess)
                latency = time.monotonic() - sym_start

                if quote.get("as_of") is None:
                    logger.warning(
                        "%s: as_of is None (price=%s, scraped_at=%s)",
                        symbol, quote.get("price"), quote.get("scraped_at"),
                    )
                else:
                    logger.info(
                        "%s: price=%.2f  as_of=%s  latency=%.2fs",
                        symbol, quote.get("price") or 0.0, quote["as_of"], latency,
                    )

                ops.append(build_upsert_doc(quote))
                success_count += 1

            except Exception as exc:
                error_count += 1
                logger.error("Failed to scrape %s: %s", symbol, exc)

            if i < len(symbols) - 1 and symbol_delay > 0:
                time.sleep(symbol_delay)

    inserted = 0
    updated  = 0
    if ops:
        try:
            result   = collection.bulk_write(ops, ordered=False)
            inserted = result.upserted_count
            updated  = result.modified_count
        except BulkWriteError as bwe:
            inserted = bwe.details.get("nUpserted", 0)
            updated  = bwe.details.get("nModified", 0)
            logger.error("BulkWriteError: %s", bwe.details.get("writeErrors", []))

    return {
        "success_count": success_count,
        "error_count":   error_count,
        "inserted":      inserted,
        "updated":       updated,
        "elapsed_s":     time.monotonic() - cycle_start,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="PSX intraday quote poller.")
    parser.add_argument("--start", type=int, default=1,
                        help="1-based start rank by marketCap desc (default: 1)")
    parser.add_argument("--end", type=int, default=None,
                        help="1-based end rank, inclusive (default: all from --start)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Source DB — symbols
    src_uri     = os.getenv("FINHISAAB_MONGO_URI", "mongodb://127.0.0.1:27017/")
    src_db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")

    # Destination DB — intraday quotes
    dst_uri     = os.getenv("MONGODB_INTRADAY_URI", "mongodb://127.0.0.1:27017/")
    dst_db_name = os.getenv("MONGODB_INTRADAY_DB_NAME", "finhisaab_intraday")

    symbol_delay   = float(os.getenv("INTRADAY_POLL_SYMBOL_DELAY", "0.5"))
    cycle_min_secs = float(os.getenv("INTRADAY_POLL_CYCLE_MIN_SECONDS", "180"))

    range_label = f"ranks {args.start}–{args.end}" if args.end else f"ranks {args.start}–end"
    logger.info("Starting intraday poller for %s", range_label)

    if not test_mongo_connectivity(src_uri, src_db_name):
        logger.error("Source MongoDB unreachable. Exiting.")
        return
    if not test_mongo_connectivity(dst_uri, dst_db_name):
        logger.error("Destination MongoDB unreachable. Exiting.")
        return

    symbols = get_stock_symbols_range(src_uri, src_db_name, args.start, args.end)
    if not symbols:
        logger.error("No symbols found for %s. Exiting.", range_label)
        return
    logger.info("Loaded %d symbols for %s.", len(symbols), range_label)

    dst_client = MongoClient(dst_uri)
    collection = dst_client[dst_db_name][INTRADAY_COLLECTION]
    ensure_indexes(collection)

    cycle_number = 0

    try:
        while True:
            sleep_until_open()

            # Guard against edge-case where we wake right at the boundary
            secs, _ = seconds_until_next_open()
            if secs > 0:
                continue

            cycle_number += 1
            logger.info(
                "=== Cycle %d started | %d symbols | %s PKT ===",
                cycle_number, len(symbols), now_pkt().strftime("%H:%M:%S"),
            )

            summary = run_poll_cycle(symbols, collection, symbol_delay)

            logger.info(
                "=== Cycle %d done | %.0fs | %d ok / %d err | %d inserted %d updated ===",
                cycle_number, summary["elapsed_s"],
                summary["success_count"], summary["error_count"],
                summary["inserted"], summary["updated"],
            )

            # Pace: if cycle finished faster than cycle_min_secs, sleep the gap
            remainder = cycle_min_secs - summary["elapsed_s"]
            if remainder > 0:
                next_secs, _ = seconds_until_next_open()
                if next_secs == 0:
                    logger.debug("Pacing: sleeping %.0fs before next cycle.", remainder)
                    time.sleep(remainder)
                # If market just closed, let sleep_until_open() handle the sleep

    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down.")
    finally:
        dst_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    main()
