"""
Continuously-running intraday poller using the /market-watch bulk endpoint.

One HTTP request per cycle fetches all ~500 PSX symbols at once.
A single instance covers the entire market — no --start/--end slicing needed.

Usage:
    python -m psx.market_watch_poller

Market closure detection (two layers):
  1. Clock-based  — sleeps overnight and on weekends using PKT market hours.
  2. Volume-stasis — during clock-open hours, if total market volume is
     identical across STASIS_THRESHOLD consecutive cycles the market is
     assumed to be on a public holiday or unexpected halt. The poller backs
     off for HOLIDAY_RECHECK_MINUTES before rechecking. Max backoff caps at
     the next scheduled clock-open so we never sleep past the next trading day.

Env vars:
    MONGODB_INTRADAY_URI          destination MongoDB URI
    MONGODB_INTRADAY_DB_NAME      destination database name (default: finhisaab_intraday)
    INTRADAY_POLL_CYCLE_MIN_SECONDS   floor between cycles in seconds (default: 60)
    INTRADAY_STASIS_THRESHOLD     frozen cycles to start the stasis clock (default: 3)
    INTRADAY_STASIS_MIN_MINUTES   wall-clock minutes volume must stay frozen before
                                  acting; prevents false positives from brief halts
                                  (default: 20)
    INTRADAY_HOLIDAY_RECHECK_MIN  minutes to sleep when stasis confirmed (default: 20)

Storage: intraday_klines_temp collection.
  Upsert key : (symbol, scraped_at_minute) — one doc per symbol per UTC minute.
  TTL        : scraped_at field, 48 hours.
  Volume     : raw `volume` is session-cumulative from PSX;
               derived `volume_delta` is computed from the previous stored snapshot.
"""

import logging
import os
import random
import time
from collections import deque
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
MARKET_OPEN  = time_type(9, 25)   # 5-min warmup before official 09:30 open
MARKET_CLOSE = time_type(15, 30)

INTRADAY_COLLECTION = "intraday_klines_temp"


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------

def test_mongo_connectivity(connection_string, db_name):
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        _ = client[db_name].name
        print(f"MongoDB connectivity OK for {connection_string} (db: {db_name})")
        return True
    except Exception as e:
        print(f"MongoDB connectivity check FAILED: {e}")
        return False
    finally:
        try:
            if "client" in locals():
                client.close()
        except Exception:
            pass


def ensure_indexes(collection):
    """
    Create indexes on intraday_klines_temp. Idempotent — safe to call every startup.

    1. Unique compound (symbol, scraped_at_minute) — upsert key.
    2. TTL on scraped_at (UTC datetime) — auto-purge after 48 h.
    """
    collection.create_index(
        [("symbol", pymongo.ASCENDING), ("scraped_at_minute", pymongo.ASCENDING)],
        unique=True,
        name="symbol_minute_unique",
    )
    collection.create_index(
        [("scraped_at", pymongo.ASCENDING)],
        expireAfterSeconds=172800,  # 48 h
        name="scraped_at_ttl",
    )
    logger.info("Indexes ensured on %s.", INTRADAY_COLLECTION)


# ---------------------------------------------------------------------------
# Clock-based market-hours logic
# ---------------------------------------------------------------------------

def now_pkt():
    return datetime.now(timezone.utc).astimezone(PKT)


def seconds_until_next_open():
    """
    Return (seconds_float, reason_str).
    Returns (0, 'open') when the clock says the market should be open.
    """
    now = now_pkt()
    wd  = now.weekday()   # 0=Mon … 4=Fri, 5=Sat, 6=Sun
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
    return (max(seconds, 0.0), f"next open {next_open.strftime('%a %Y-%m-%d %H:%M')} PKT")


def _chunked_sleep(target_utc):
    """Sleep in 60-second chunks until target_utc, responding to Ctrl-C quickly."""
    while True:
        remaining = (target_utc - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            break
        time.sleep(min(remaining, 60.0))


def sleep_until_open():
    """Block until the clock says the market is open."""
    seconds, reason = seconds_until_next_open()
    if seconds <= 0:
        return
    target_utc = datetime.now(timezone.utc) + timedelta(seconds=seconds)
    logger.info(
        "Market closed (%s). Sleeping %.0f min until ~%s PKT.",
        reason, seconds / 60,
        target_utc.astimezone(PKT).strftime("%Y-%m-%d %H:%M"),
    )
    _chunked_sleep(target_utc)
    logger.info("Waking up — market should be open now.")


# ---------------------------------------------------------------------------
# Volume-stasis holiday detection
# ---------------------------------------------------------------------------

class StasisDetector:
    """
    Tracks total market volume across recent cycles.

    Detection is two-stage to avoid false positives from brief trading halts:

    Stage 1 — cycle gate: the last `threshold` cycles must all report the same
      total volume (and volume > 0 to exclude the pre-open window).

    Stage 2 — wall-clock gate: once Stage 1 trips, a timer starts. Stasis is
      only confirmed after `min_stasis_minutes` of continuously frozen volume.
      Any movement in volume resets both stages.

    Only active during clock-open hours — the clock-based sleep handles
    nights and weekends, so stasis detection is the second layer for holidays.

    After stasis is confirmed:
      - sleeps for `recheck_minutes`, capped so we never sleep past the next
        scheduled clock-open (avoids sleeping through the next trading day).
      - resets its history so the next cycle starts fresh.
    """

    def __init__(self, threshold, recheck_minutes, min_stasis_minutes=20):
        self.threshold          = threshold
        self.recheck_minutes    = recheck_minutes
        self.min_stasis_minutes = min_stasis_minutes
        self._recent_volumes    = deque(maxlen=threshold)
        self._stasis_since      = None  # UTC time when Stage 1 first tripped

    def record(self, total_volume):
        """Call after every successful cycle with the sum of all symbol volumes."""
        self._recent_volumes.append(total_volume)

    def is_stale(self):
        """
        True when Stage 1 (cycle gate) and Stage 2 (wall-clock gate) are both met.
        Resets the wall-clock timer whenever volume moves again.
        """
        if len(self._recent_volumes) < self.threshold:
            return False
        volumes = list(self._recent_volumes)
        if volumes[0] == 0:
            return False  # pre-open, no trades yet

        if len(set(volumes)) != 1:
            # Volume moved — reset the timer so a future freeze starts fresh
            if self._stasis_since is not None:
                logger.debug("Volume moved; stasis timer reset.")
                self._stasis_since = None
            return False

        # Stage 1 passed — start (or continue) the wall-clock gate
        now = datetime.now(timezone.utc)
        if self._stasis_since is None:
            self._stasis_since = now
            logger.info(
                "Volume stasis Stage 1: %d consecutive frozen cycles. "
                "Waiting %.0f min wall-clock before acting.",
                self.threshold, self.min_stasis_minutes,
            )

        elapsed_min = (now - self._stasis_since).total_seconds() / 60
        return elapsed_min >= self.min_stasis_minutes

    def sleep_and_reset(self):
        """
        Sleep for recheck_minutes (or until next clock-open, whichever is sooner),
        then clear history so the next cycle evaluates fresh data.
        """
        recheck_secs       = self.recheck_minutes * 60
        clock_secs, _      = seconds_until_next_open()
        # If market already closed by clock, use clock sleep instead
        if clock_secs > 0:
            sleep_secs = clock_secs
            label      = "clock-open"
        else:
            # Cap recheck at whatever time remains until clock-close,
            # so we never accidentally sleep past 15:30 on a thin trading day
            close_dt = now_pkt().replace(hour=15, minute=30, second=0, microsecond=0)
            secs_to_close = (close_dt - now_pkt()).total_seconds()
            sleep_secs = min(recheck_secs, max(secs_to_close, 60))
            label      = "holiday/halt recheck"

        target_utc = datetime.now(timezone.utc) + timedelta(seconds=sleep_secs)
        elapsed_min = (
            (datetime.now(timezone.utc) - self._stasis_since).total_seconds() / 60
            if self._stasis_since else 0
        )
        logger.warning(
            "Volume stasis confirmed — frozen for %.0f min across %d cycles. "
            "Possible holiday or halt. Sleeping %.0f min (%s) until ~%s PKT.",
            elapsed_min, self.threshold, sleep_secs / 60, label,
            target_utc.astimezone(PKT).strftime("%H:%M"),
        )
        _chunked_sleep(target_utc)
        logger.info("Recheck after stasis sleep — resuming polling.")
        self._recent_volumes.clear()
        self._stasis_since = None


# ---------------------------------------------------------------------------
# Document building
# ---------------------------------------------------------------------------

def _minute_floor(dt_utc):
    return dt_utc.replace(second=0, microsecond=0)


def _pkt_date(dt_utc):
    return dt_utc.astimezone(PKT).date()


def load_previous_snapshots(collection, symbols, current_minute):
    """
    Return the latest stored snapshot before current_minute for each symbol.

    Uses the existing (symbol, scraped_at_minute) index order so the lookup
    stays aligned with how documents are keyed in MongoDB.
    """
    if not symbols:
        return {}

    pipeline = [
        {
            "$match": {
                "symbol": {"$in": sorted(set(symbols))},
                "scraped_at_minute": {"$lt": current_minute},
            }
        },
        {"$sort": {"symbol": 1, "scraped_at_minute": 1}},
        {
            "$group": {
                "_id": "$symbol",
                "volume": {"$last": "$volume"},
                "scraped_at_minute": {"$last": "$scraped_at_minute"},
            }
        },
    ]

    previous = {}
    for doc in collection.aggregate(pipeline):
        previous[doc["_id"]] = {
            "volume": doc.get("volume"),
            "scraped_at_minute": doc.get("scraped_at_minute"),
        }
    return previous


def compute_volume_delta(current_volume, previous_snapshot, current_minute):
    """
    Derive interval volume from cumulative session volume snapshots.

    Returns None when there is no trustworthy prior point, such as:
      - the first retained snapshot for a symbol
      - a session/day rollover
      - a source reset where cumulative volume moved backwards
    """
    if current_volume is None or previous_snapshot is None:
        return None

    previous_volume = previous_snapshot.get("volume")
    previous_minute = previous_snapshot.get("scraped_at_minute")
    if previous_volume is None or previous_minute is None:
        return None

    if _pkt_date(previous_minute) != _pkt_date(current_minute):
        return None

    if current_volume < previous_volume:
        return None

    return current_volume - previous_volume


def build_ops(quotes, scraped_at, previous_by_symbol):
    """
    Convert quote dicts from fetch_market_watch() into UpdateOne ops.

    Upsert key : (symbol, scraped_at_minute)
    TTL field  : scraped_at (exact UTC datetime)
    """
    ops     = []
    now_utc = datetime.now(timezone.utc)
    minute  = _minute_floor(scraped_at)

    for q in quotes:
        volume_delta = compute_volume_delta(
            q.get("volume"),
            previous_by_symbol.get(q["symbol"]),
            minute,
        )
        ops.append(
            pymongo.UpdateOne(
                {"symbol": q["symbol"], "scraped_at_minute": minute},
                {
                    "$set": {
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
                        "volume_delta":     volume_delta,
                        "scraped_at":       scraped_at,
                        "updated_at":       now_utc,
                    },
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
    """One HTTP request → parse → bulk upsert. Returns a summary dict."""
    cycle_start = time.monotonic()

    try:
        quotes, scraped_at = fetch_market_watch(session=session)
    except Exception as exc:
        logger.error("fetch_market_watch failed: %s", exc)
        return {
            "symbol_count": 0, "total_volume": None,
            "inserted": 0, "updated": 0,
            "elapsed_s": time.monotonic() - cycle_start,
            "fetch_error": True,
        }

    total_volume = sum(q.get("volume") or 0 for q in quotes)
    minute       = _minute_floor(scraped_at)
    previous_by_symbol = load_previous_snapshots(
        collection,
        [q["symbol"] for q in quotes if q.get("symbol")],
        minute,
    )
    ops          = build_ops(quotes, scraped_at, previous_by_symbol)
    inserted = updated = 0

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
        "total_volume": total_volume,
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
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    dst_uri         = os.getenv("MONGODB_INTRADAY_URI", "mongodb://127.0.0.1:27017/")
    dst_db_name     = os.getenv("MONGODB_INTRADAY_DB_NAME", "finhisaab_intraday")
    cycle_min_secs  = float(os.getenv("INTRADAY_POLL_CYCLE_MIN_SECONDS", "90"))
    cycle_max_secs  = float(os.getenv("INTRADAY_POLL_CYCLE_MAX_SECONDS", "150"))
    stasis_thresh       = int(os.getenv("INTRADAY_STASIS_THRESHOLD", "3"))
    stasis_min_minutes  = float(os.getenv("INTRADAY_STASIS_MIN_MINUTES", "20"))
    holiday_recheck     = float(os.getenv("INTRADAY_HOLIDAY_RECHECK_MIN", "20"))

    logger.info(
        "Starting market-watch poller | cycle range: %.0f–%.0fs | "
        "stasis: %d cycles + %.0f min wall-clock | holiday recheck: %.0f min.",
        cycle_min_secs, cycle_max_secs, stasis_thresh, stasis_min_minutes, holiday_recheck,
    )

    if not test_mongo_connectivity(dst_uri, dst_db_name):
        logger.error("MongoDB unreachable. Exiting.")
        return

    dst_client = MongoClient(dst_uri)
    collection = dst_client[dst_db_name][INTRADAY_COLLECTION]
    ensure_indexes(collection)

    stasis   = StasisDetector(stasis_thresh, holiday_recheck, stasis_min_minutes)
    cycle_number = 0

    try:
        with requests.Session() as sess:
            while True:
                # Layer 1: clock-based sleep (nights, weekends)
                sleep_until_open()

                secs, _ = seconds_until_next_open()
                if secs > 0:
                    continue

                cycle_number += 1
                logger.info("=== Cycle %d | %s PKT ===",
                            cycle_number, now_pkt().strftime("%H:%M:%S"))

                summary = run_poll_cycle(collection, sess)

                logger.info(
                    "=== Cycle %d done | %.1fs | %d symbols | vol=%s | "
                    "%d inserted %d updated%s ===",
                    cycle_number, summary["elapsed_s"], summary["symbol_count"],
                    f"{summary['total_volume']:,}" if summary["total_volume"] is not None else "n/a",
                    summary["inserted"], summary["updated"],
                    " | FETCH ERROR" if summary["fetch_error"] else "",
                )

                # Layer 2: volume-stasis holiday detection
                if not summary["fetch_error"] and summary["total_volume"] is not None:
                    stasis.record(summary["total_volume"])
                    if stasis.is_stale():
                        stasis.sleep_and_reset()
                        cycle_number = 0  # reset so logs are readable after each wake
                        continue

                # Pace to a random interval within [cycle_min_secs, cycle_max_secs]
                target_secs = random.uniform(cycle_min_secs, cycle_max_secs)
                remainder   = target_secs - summary["elapsed_s"]
                if remainder > 0:
                    next_secs, _ = seconds_until_next_open()
                    if next_secs == 0:
                        logger.debug("Pacing: sleeping %.0fs before next cycle.", remainder)
                        time.sleep(remainder)

    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down.")
    finally:
        dst_client.close()
        logger.info("MongoDB connection closed.")


if __name__ == "__main__":
    main()
