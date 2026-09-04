"""Incremental and EOD materialization for durable stock intraday candles."""

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from pymongo import UpdateOne

from psx.intraday_candles import (
    HIGHER_TIMEFRAMES,
    PSX_FEED_DELAY_MS,
    bar_lifecycle,
    build_higher_timeframe_candles,
    build_one_minute_candles,
    candle_differs,
    lifecycle_differs,
)


SEED_LOOKBACK_MS = 5 * 60 * 1000
TIMEFRAME = "1m"
DEFAULT_SYMBOL_BATCH_SIZE = 50
DEFAULT_RETENTION_DAYS = {
    "1m": 15,
    "5m": 30,
    "15m": 90,
    "30m": 180,
    "1h": 365,
    "4h": 730,
}


def _as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _date_bounds(trading_date):
    day = datetime.strptime(trading_date, "%Y-%m-%d").date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone(timedelta(hours=5)))
    return start.astimezone(timezone.utc), (start + timedelta(days=1)).astimezone(timezone.utc)


def _datetime_from_ms(timestamp):
    return datetime.fromtimestamp(timestamp / 1000, timezone.utc)


def _merge_changed(target, source):
    for timeframe, by_symbol in source.items():
        target_symbols = target.setdefault(timeframe, {})
        for symbol, entry in by_symbol.items():
            if symbol in target_symbols:
                target_symbols[symbol]["minTimestamp"] = min(
                    target_symbols[symbol]["minTimestamp"], entry["minTimestamp"]
                )
                target_symbols[symbol]["count"] += entry["count"]
            else:
                target_symbols[symbol] = dict(entry)


def _changed_by_timeframe(changed):
    return {
        timeframe: sorted(by_symbol)
        for timeframe, by_symbol in sorted(changed.items())
    }


class IntradayMaterializer:
    """Materialize raw market-watch snapshots into backend-compatible candles."""

    def __init__(self, raw_collection, candle_collection, symbol_batch_size=DEFAULT_SYMBOL_BATCH_SIZE):
        self.raw_collection = raw_collection
        self.candle_collection = candle_collection
        self.symbol_batch_size = max(1, int(symbol_batch_size))

    def ensure_indexes(self):
        self.candle_collection.create_index(
            [("symbol", 1), ("timeframe", 1), ("timestamp", 1)],
            unique=True,
            name="symbol_timeframe_timestamp_unique",
        )
        self.candle_collection.create_index(
            [("symbol", 1), ("timeframe", 1), ("tradingDate", 1), ("timestamp", -1)],
            name="symbol_timeframe_trading_date_timestamp",
        )
        self.candle_collection.create_index(
            [("timeframe", 1), ("tradingDate", -1)],
            name="timeframe_trading_date",
        )

    def _persist_candles(self, candles_by_timeframe, now, data_as_of, trading_date):
        examined = 0
        written = 0
        settled = 0
        changed = {}
        operations = []

        for timeframe, candles in candles_by_timeframe.items():
            if not candles:
                continue

            symbols = sorted({candle["symbol"] for candle in candles})
            min_timestamp = min(candle["timestamp"] for candle in candles)
            max_timestamp = max(candle["timestamp"] for candle in candles)
            existing_rows = self.candle_collection.find(
                {
                    "symbol": {"$in": symbols},
                    "timeframe": timeframe,
                    "tradingDate": trading_date,
                    "timestamp": {"$gte": min_timestamp, "$lte": max_timestamp},
                },
                {
                    "_id": 0,
                    "symbol": 1,
                    "timestamp": 1,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "isForming": 1,
                    "barCloseAt": 1,
                },
            )
            existing_by_key = {
                (row["symbol"], row["timestamp"]): row for row in existing_rows
            }

            for candle in candles:
                examined += 1
                key = (candle["symbol"], candle["timestamp"])
                existing = existing_by_key.get(key)
                lifecycle = bar_lifecycle(candle["timestamp"], timeframe, data_as_of)

                if candle_differs(candle, existing):
                    document = {**candle, **lifecycle, "updatedAt": now}
                    operations.append(
                        UpdateOne(
                            {
                                "symbol": candle["symbol"],
                                "timeframe": timeframe,
                                "timestamp": candle["timestamp"],
                            },
                            {"$set": document},
                            upsert=True,
                        )
                    )
                    written += 1
                    by_symbol = changed.setdefault(timeframe, {})
                    entry = by_symbol.get(candle["symbol"])
                    if entry:
                        entry["minTimestamp"] = min(entry["minTimestamp"], candle["timestamp"])
                        entry["count"] += 1
                    else:
                        by_symbol[candle["symbol"]] = {
                            "minTimestamp": candle["timestamp"],
                            "count": 1,
                        }
                elif existing and lifecycle_differs(existing, lifecycle):
                    operations.append(
                        UpdateOne(
                            {
                                "symbol": candle["symbol"],
                                "timeframe": timeframe,
                                "timestamp": candle["timestamp"],
                            },
                            {"$set": lifecycle},
                        )
                    )
                    settled += 1

        if operations:
            self.candle_collection.bulk_write(operations, ordered=False)

        return {
            "examined": examined,
            "written": written,
            "settled": settled,
            "changed": changed,
        }

    def _persist_one_minute_batches(self, snapshots_by_symbol, now, data_as_of, trading_date):
        total = {"examined": 0, "written": 0, "settled": 0, "changed": {}}
        symbols = list(snapshots_by_symbol)
        for offset in range(0, len(symbols), self.symbol_batch_size):
            batch = symbols[offset:offset + self.symbol_batch_size]
            candles = []
            for symbol in batch:
                candles.extend(build_one_minute_candles(snapshots_by_symbol[symbol]))
            result = self._persist_candles(
                {TIMEFRAME: candles}, now, data_as_of, trading_date
            )
            total["examined"] += result["examined"]
            total["written"] += result["written"]
            total["settled"] += result["settled"]
            _merge_changed(total["changed"], result["changed"])
        return total

    def _load_raw_snapshots(self, trading_date, minimum_scrape_time=None):
        start, end = _date_bounds(trading_date)
        query = {"scraped_at": {"$gte": start, "$lt": end}}
        if minimum_scrape_time is not None:
            query["scraped_at_minute"] = {"$gte": minimum_scrape_time}
        # Keep the scraped_at range indexable; grouping preserves per-symbol
        # order after the global minute sort.
        rows = self.raw_collection.find(query).sort([("scraped_at_minute", 1)])
        by_symbol = defaultdict(list)
        for row in rows:
            by_symbol[row["symbol"]].append(row)
        return dict(by_symbol)

    def rollup_trading_date(self, trading_date, now=None):
        now = _as_utc(now or datetime.now(timezone.utc))
        data_as_of = now - timedelta(milliseconds=PSX_FEED_DELAY_MS)
        snapshots_by_symbol = self._load_raw_snapshots(trading_date)
        if not snapshots_by_symbol:
            return {
                "tradingDate": trading_date,
                "symbolsProcessed": 0,
                "totalCandles": 0,
                "examined": 0,
                "written": 0,
                "settled": 0,
                "changedByTimeframe": {},
                "changedPairCount": 0,
            }

        one_minute = self._persist_one_minute_batches(
            snapshots_by_symbol, now, data_as_of, trading_date
        )
        higher = self.rollup_higher_timeframes(trading_date, now, data_as_of)
        changed = {}
        _merge_changed(changed, one_minute["changed"])
        _merge_changed(changed, higher["changed"])
        changed_by_timeframe = _changed_by_timeframe(changed)
        return {
            "tradingDate": trading_date,
            "symbolsProcessed": len(snapshots_by_symbol),
            "totalCandles": one_minute["examined"],
            "examined": one_minute["examined"] + higher["examined"],
            "written": one_minute["written"] + higher["written"],
            "settled": one_minute["settled"] + higher["settled"],
            "changedByTimeframe": changed_by_timeframe,
            "changedPairCount": sum(len(symbols) for symbols in changed_by_timeframe.values()),
        }

    def rollup_higher_timeframes(self, trading_date, now=None, data_as_of=None):
        now = _as_utc(now or datetime.now(timezone.utc))
        data_as_of = data_as_of or (now - timedelta(milliseconds=PSX_FEED_DELAY_MS))
        symbols = self.candle_collection.distinct(
            "symbol", {"timeframe": TIMEFRAME, "tradingDate": trading_date}
        )
        total = {"examined": 0, "written": 0, "settled": 0, "changed": {}}

        for offset in range(0, len(symbols), self.symbol_batch_size):
            batch = symbols[offset:offset + self.symbol_batch_size]
            rows = self.candle_collection.find(
                {"symbol": {"$in": batch}, "timeframe": TIMEFRAME, "tradingDate": trading_date}
            ).sort([("symbol", 1), ("timestamp", 1)])
            by_symbol = defaultdict(list)
            for row in rows:
                by_symbol[row["symbol"]].append(row)

            candles_by_timeframe = {label: [] for label, _ in HIGHER_TIMEFRAMES}
            for symbol_rows in by_symbol.values():
                for label, minutes in HIGHER_TIMEFRAMES:
                    candles_by_timeframe[label].extend(
                        build_higher_timeframe_candles(symbol_rows, label, minutes)
                    )

            result = self._persist_candles(
                candles_by_timeframe, now, data_as_of, trading_date
            )
            total["examined"] += result["examined"]
            total["written"] += result["written"]
            total["settled"] += result["settled"]
            _merge_changed(total["changed"], result["changed"])
        return total

    def rollup_incremental(self, trading_date, now=None, window_minutes=20):
        now = _as_utc(now or datetime.now(timezone.utc))
        day_start, _ = _date_bounds(trading_date)
        now_ms = int(now.timestamp() * 1000)
        day_start_ms = int(day_start.timestamp() * 1000)
        window_start_ms = max(day_start_ms, now_ms - int(window_minutes * 60 * 1000))
        read_from_ms = max(
            day_start_ms,
            window_start_ms + PSX_FEED_DELAY_MS - SEED_LOOKBACK_MS,
        )
        snapshots_by_symbol = self._load_raw_snapshots(
            trading_date, _datetime_from_ms(read_from_ms)
        )

        one_minute_by_symbol = {}
        for symbol, snapshots in snapshots_by_symbol.items():
            candles = [
                candle for candle in build_one_minute_candles(snapshots)
                if candle["timestamp"] >= window_start_ms
            ]
            if candles:
                one_minute_by_symbol[symbol] = candles

        data_as_of = now - timedelta(milliseconds=PSX_FEED_DELAY_MS)
        one_minute = self._persist_candles(
            {TIMEFRAME: [candle for candles in one_minute_by_symbol.values() for candle in candles]},
            now,
            data_as_of,
            trading_date,
        )
        changed_one_minute = one_minute["changed"].get(TIMEFRAME, {})
        changed_symbols = list(changed_one_minute)
        higher = {"examined": 0, "written": 0, "settled": 0, "changed": {}}

        if changed_symbols:
            coarsest_ms = max(minutes for _, minutes in HIGHER_TIMEFRAMES) * 60 * 1000
            bucket_read_from_ms = max(
                day_start_ms,
                min(
                    (entry["minTimestamp"] // coarsest_ms) * coarsest_ms
                    for entry in changed_one_minute.values()
                ),
            )
            rows = self.candle_collection.find(
                {
                    "symbol": {"$in": changed_symbols},
                    "timeframe": TIMEFRAME,
                    "tradingDate": trading_date,
                    "timestamp": {"$gte": bucket_read_from_ms},
                }
            ).sort([("symbol", 1), ("timestamp", 1)])
            persisted_by_symbol = defaultdict(list)
            for row in rows:
                persisted_by_symbol[row["symbol"]].append(row)

            candles_by_timeframe = {label: [] for label, _ in HIGHER_TIMEFRAMES}
            for symbol in changed_symbols:
                changed_from = changed_one_minute[symbol]["minTimestamp"]
                for label, minutes in HIGHER_TIMEFRAMES:
                    bucket_ms = minutes * 60 * 1000
                    floor = (changed_from // bucket_ms) * bucket_ms
                    members = [
                        row for row in persisted_by_symbol[symbol]
                        if row["timestamp"] >= floor
                    ]
                    candles_by_timeframe[label].extend(
                        build_higher_timeframe_candles(members, label, minutes)
                    )

            higher = self._persist_candles(
                candles_by_timeframe, now, data_as_of, trading_date
            )

        changed = {}
        _merge_changed(changed, one_minute["changed"])
        _merge_changed(changed, higher["changed"])
        changed_by_timeframe = _changed_by_timeframe(changed)
        return {
            "tradingDate": trading_date,
            "symbolsProcessed": len(snapshots_by_symbol),
            "oneMinCandles": sum(len(candles) for candles in one_minute_by_symbol.values()),
            "higherCandles": higher["examined"],
            "examined": one_minute["examined"] + higher["examined"],
            "written": one_minute["written"] + higher["written"],
            "settled": one_minute["settled"] + higher["settled"],
            "changedByTimeframe": changed_by_timeframe,
            "changedPairCount": sum(len(symbols) for symbols in changed_by_timeframe.values()),
        }

    def prune_raw_snapshots(self, keep_trading_days=7):
        dates = [row["_id"] for row in self.raw_collection.aggregate([
            {
                "$group": {
                    "_id": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$scraped_at",
                            "timezone": "Asia/Karachi",
                        }
                    }
                }
            },
            {"$sort": {"_id": -1}},
        ])]
        if len(dates) < keep_trading_days:
            return {"deletedCount": 0, "reason": "insufficient_trading_days"}
        cutoff, _ = _date_bounds(dates[keep_trading_days - 1])
        result = self.raw_collection.delete_many({"scraped_at": {"$lt": cutoff}})
        return {"deletedCount": result.deleted_count, "cutoffDate": dates[keep_trading_days - 1]}

    def prune_candles(self, retention_days=None):
        retention_days = retention_days or DEFAULT_RETENTION_DAYS
        result = {"deletedCount": 0, "byTimeframe": {}}
        for timeframe, days in retention_days.items():
            if not days or days <= 0:
                result["byTimeframe"][timeframe] = {"deletedCount": 0, "reason": "disabled"}
                continue
            dates = sorted(
                self.candle_collection.distinct("tradingDate", {"timeframe": timeframe}),
                reverse=True,
            )
            if len(dates) < days:
                result["byTimeframe"][timeframe] = {
                    "deletedCount": 0,
                    "reason": "insufficient_trading_days",
                }
                continue
            cutoff = dates[days - 1]
            deleted = self.candle_collection.delete_many(
                {"timeframe": timeframe, "tradingDate": {"$lt": cutoff}}
            ).deleted_count
            result["byTimeframe"][timeframe] = {"deletedCount": deleted, "cutoff": cutoff}
            result["deletedCount"] += deleted
        return result


def build_materializer_from_database(raw_db, company_data_db, symbol_batch_size=DEFAULT_SYMBOL_BATCH_SIZE):
    return IntradayMaterializer(
        raw_db["intraday_klines_temp"],
        company_data_db["intraday_klines"],
        symbol_batch_size=symbol_batch_size,
    )
