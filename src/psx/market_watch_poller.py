"""
Continuously-running intraday poller using the /market-watch bulk endpoint.

One HTTP request per cycle fetches all ~500 PSX symbols at once, vs the
per-symbol approach in intraday_poller.py. No --start/--end slicing needed —
a single instance covers the entire market.

Usage:
    python market_watch_poller.py

Stores snapshots in intraday_klines_temp (MONGODB_INTRADAY_URI).
Documents expire automatically after 48 hours via a TTL index on scraped_at.
Upsert key: (symbol, scraped_at_minute) — one document per symbol per UTC minute,
so rapid re-polls within the same minute are idempotent.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from datetime import time as time_type

import pymongo
import requests
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from psx.market_watch_scraper import fetch_market_watch

logger = logging.getLogger(__name__)

PKT          = timezone(timedelta(hours=5))
MARKET_OPEN  = time_type(9, 25)
MARKET_CLOSE = time_type(15, 30)

INTRADAY_COLLECTION = "intraday_klines_temp"


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def test_mongo_connectivity(connection_string, db_name):
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
    Indexes on intraday_klines_temp. Idempotent — safe to call every startup.

    Upsert key: (symbol, scraped_at_minute) — UTC datetime truncated to the
    minute. One document per symbol per minute; rapid re-polls within the same
    minute overwrite the same doc.

    TTL on scraped_at — auto-purge raw UTC datetime after 48 h.
    """
    collection.create_index(
        [("symbol", pymongo.ASCENDING), ("scraped_at_minute", pymongo.ASCENDING)],
        unique=True,
        name="symbol_minute_unique",
    )
    collection.create_index(
        [("scraped_at", pymongo.ASCENDING)],
        expireAfterSeconds=172800,   # 48 h
        name="scraped_at_ttl",
    )
    logger.info("Indexes ensured on %s.", INTRADAY_COLLECTION)


# ---------------------------------------------------------------------------
# Market-hours logic  (identical to intraday_poller.py)
# ---------------------------------------------------------------------------

def now_pkt():
    return datetime.now(timezone.utc).astimezone(PKT)


def seconds_until_next_open():
    """
    Return (seconds_float, reason_str).
    Returns (0, 'open') during PSX market hours.
    """
    now = now_pkt()
    wd  = now.weekday()
    t   = now.time().replace(second=0, microsecond=0)

    if wd <= 4 and MARKET_OPEN <= t < MARKET_CLOSE:
        return (0.0, "open")

    if wd <= 4 and t < MARKET_OPEN:
        next_open = now.replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd <= 3 and t >= MARKET_CLOSE:
        next_open = (now + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd == 4 and t >= MARKET_CLOSE:
        next_open = (now + timedelta(days=3)).replace(hour=9, minute=25, second=0, microsecond=0)
    elif wd == 5:
        next_open = (now + timedelta(days=2)).replace(hour=9, minute=25, second=0, microsecond=0)
    else:
        next_open = (now + timedelta(days=1)).replace(hour=9, minute=25, second=0, microsecond=0)

    seconds = (next_open - now).total_seconds()
    reason  = f"next open {next_open.strftime('%a %Y-%m-%d %H:%M')} PKT"
    return (max(seconds, 0.0), reason)


def sleep_until_open():
    seconds, reason = seconds_until_next_open()
    if seconds <= 0:
        return
    target_utc = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    logger.info(
        "Market closed (%s). Sleeping %.0f min until ~%s PKT.",
        reason, seconds / 60,
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

def _minute_floor(dt_utc):
    """Truncate a UTC-aware datetime to the minute — used as the upsert bucket."""
    return dt_utc.replace(second=0, microsecond=0)


def build_ops(quotes, scraped_at):
    """
    Convert the list of quote dicts from fetch_market_watch() into
    a list of UpdateOne operations ready for bulk_write.

    Upsert key: (symbol, scraped_at_minute)
      - scraped_at_minute  — UTC datetime floored to the minute
      - scraped_at         — exact UTC datetime (kept for TTL index and audit)
    """
    ops     = []
    now_utc = datetime.now(timezone.utc)
    minute  = _minute_floor(scraped_at)

    for q in quotes:
        set_fields = {
            "name":             q.get("name"),
            "sector_code":      q.get("sector_code"),
            "indices":          q.get("indices"),
            "ldcp":             q.get("ldcp"),
            "open":             q.get("open"),
            "high":             q.get("high"),
            "low":              q.get("low"),
            "price":            q.get("price"),
            "change":           q.get("change"),
            "change_pct":       q.get("change_pct"),
            "change_direction": q.get("change_direction"),
            "volume":           q.get("volume"),
            "scraped_at":       scraped_at,      # exact UTC — TTL field
            "updated_at":       now_utc,
        }
        ops.append(
            pymongo.UpdateOne(
                {"symbol": q["symbol"], "scraped_at_minute": minute},
                {
                    "$set":         set_fields,
                    "$setOnInsert": {"created_at": now_utc},
                },
                upsert=True,
            )
        )
    return ops


# ---------------------------------------------------------------------------
# Poll cycle
# ---------------------------------------------------------------------------

def run_poll_cycle(collection, session):
    """
    One HTTP request → parse → bulk upsert. Returns a summary dict.
    """
    cycle_start = time.monotonic()

    try:
        quotes, scraped_at = fetch_market_watch(session=session)
    except Exception as exc:
        logger.error("fetch_market_watch failed: %s", exc)
        return {"symbol_count": 0, "inserted": 0, "updated": 0,
                "elapsed_s": time.monotonic() - cycle_start, "fetch_error": True}

    ops      = build_ops(quotes, scraped_at)
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
        "symbol_count": len(quotes),
        "inserted":     inserted,
        "updated":      updated,
        "elapsed_s":    time.monotonic() - cycle_start,
        "fetch_error":  False,
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="PSX market-watch intraday poller.")
    parser.add_argument("--cycle-seconds", type=float, default=None,
                        help="Minimum seconds between cycles (default: INTRADAY_POLL_CYCLE_MIN_SECONDS env var or 60)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    dst_uri        = os.getenv("MONGODB_INTRADAY_URI", "mongodb://127.0.0.1:27017/")
    dst_db_name    = os.getenv("MONGODB_INTRADAY_DB_NAME", "finhisaab_intraday")
    cycle_min_secs = args.cycle_seconds or float(os.getenv("INTRADAY_POLL_CYCLE_MIN_SECONDS", "60"))

    logger.info("Starting market-watch poller (cycle floor: %.0fs).", cycle_min_secs)

    if not test_mongo_connectivity(dst_uri, dst_db_name):
        logger.error("MongoDB unreachable. Exiting.")
        return

    dst_client = MongoClient(dst_uri)
    collection = dst_client[dst_db_name][INTRADAY_COLLECTION]
    ensure_indexes(collection)

    cycle_number = 0

    try:
        with requests.Session() as sess:
            while True:
                sleep_until_open()

                secs, _ = seconds_until_next_open()
                if secs > 0:
                    continue

                cycle_number += 1
                logger.info(
                    "=== Cycle %d | %s PKT ===",
                    cycle_number, now_pkt().strftime("%H:%M:%S"),
                )

                summary = run_poll_cycle(collection, sess)

                logger.info(
                    "=== Cycle %d done | %.1fs | %d symbols | %d inserted %d updated%s ===",
                    cycle_number, summary["elapsed_s"], summary["symbol_count"],
                    summary["inserted"], summary["updated"],
                    " | FETCH ERROR" if summary["fetch_error"] else "",
                )

                remainder = cycle_min_secs - summary["elapsed_s"]
                if remainder > 0:
                    next_secs, _ = seconds_until_next_open()
                    if next_secs == 0:
                        logger.debug("Pacing: sleeping %.0fs.", remainder)
                        time.sleep(remainder)

    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down.")
    finally:
        dst_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    main()
