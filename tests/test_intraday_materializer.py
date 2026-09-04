import unittest
from datetime import datetime, timedelta, timezone

from psx.intraday_candles import bar_close_ms
from psx.intraday_materializer import IntradayMaterializer


class FakeCollection:
    def __init__(self, documents=None):
        self.documents = list(documents or [])
        self.operations = []
        self.sort_calls = []

    def find(self, query, projection=None):
        rows = []
        for document in self.documents:
            if query.get("timeframe") and document.get("timeframe") != query["timeframe"]:
                continue
            trading_date = query.get("tradingDate")
            if isinstance(trading_date, str) and document.get("tradingDate") != trading_date:
                continue
            symbols = query.get("symbol", {}).get("$in")
            if symbols is not None and document.get("symbol") not in symbols:
                continue
            bounds = query.get("timestamp", {})
            if "$gte" in bounds and document["timestamp"] < bounds["$gte"]:
                continue
            if "$lte" in bounds and document["timestamp"] > bounds["$lte"]:
                continue
            date_bounds = query.get("tradingDate", {})
            if isinstance(date_bounds, dict) and "$lt" in date_bounds and document["tradingDate"] >= date_bounds["$lt"]:
                continue
            rows.append(document.copy())

        collection = self

        class Cursor(list):
            def sort(self, fields):
                collection.sort_calls.append(fields)
                for field, direction in reversed(fields):
                    super().sort(key=lambda row: row[field], reverse=direction < 0)
                return self

        return Cursor(rows)

    def bulk_write(self, operations, ordered=False):
        self.operations.extend(operations)
        for operation in operations:
            selector = operation._filter
            document = next(
                (
                    row for row in self.documents
                    if all(row.get(key) == value for key, value in selector.items())
                ),
                None,
            )
            update = operation._doc["$set"]
            if document is None:
                document = dict(update)
                self.documents.append(document)
            else:
                document.update(update)

    def distinct(self, field, query):
        return sorted({row[field] for row in self.documents if all(
            row.get(key) == value for key, value in query.items()
        )})

    def delete_many(self, query):
        before = len(self.documents)
        remaining = []
        for row in self.documents:
            if query.get("timeframe") and row.get("timeframe") != query["timeframe"]:
                remaining.append(row)
                continue
            cutoff = query.get("tradingDate", {}).get("$lt")
            if cutoff is None or row.get("tradingDate", "") >= cutoff:
                remaining.append(row)
        self.documents = remaining

        class DeleteResult:
            deleted_count = before - len(remaining)

        return DeleteResult()


class MaterializerTests(unittest.TestCase):
    def test_eod_rollup_materializes_one_minute_and_all_higher_timeframes(self):
        pkt = timezone(timedelta(hours=5))

        def raw(text, price, volume, delta):
            moment = datetime.fromisoformat(text).replace(tzinfo=pkt)
            return {
                "symbol": "ABC",
                "scraped_at": moment,
                "scraped_at_minute": moment,
                "price": price,
                "open": 100,
                "high": price,
                "low": 100,
                "volume": volume,
                "volume_delta": delta,
            }

        raw_collection = FakeCollection([
            raw("2026-08-28T09:31:00", 101, 1000, None),
            raw("2026-08-28T09:32:00", 102, 1100, 100),
        ])
        candle_collection = FakeCollection()
        materializer = IntradayMaterializer(raw_collection, candle_collection)

        result = materializer.rollup_trading_date(
            "2026-08-28",
            now=datetime(2026, 8, 28, 12, tzinfo=timezone.utc),
        )

        self.assertEqual(result["symbolsProcessed"], 1)
        self.assertEqual(result["totalCandles"], 2)
        self.assertEqual(
            {row["timeframe"] for row in candle_collection.documents},
            {"1m", "5m", "15m", "30m", "1h", "4h"},
        )
        self.assertEqual(
            sum(row["volume"] for row in candle_collection.documents if row["timeframe"] == "1m"),
            1100.0,
        )

        repeat = materializer.rollup_trading_date(
            "2026-08-28",
            now=datetime(2026, 8, 28, 12, tzinfo=timezone.utc),
        )
        self.assertEqual(repeat["written"], 0)
        self.assertEqual(repeat["settled"], 0)
        self.assertEqual(repeat["changedByTimeframe"], {})

    def test_raw_snapshot_read_sorts_by_minute_without_leading_symbol(self):
        raw_collection = FakeCollection([])
        materializer = IntradayMaterializer(raw_collection, FakeCollection())

        materializer._load_raw_snapshots("2026-08-28")

        # The date range remains usable by scraped_at_1; grouping restores the
        # per-symbol order needed by the candle builder.
        self.assertEqual(raw_collection.sort_calls, [[("scraped_at_minute", 1)]])

    def test_candle_pruning_keeps_configured_number_of_trading_days(self):
        candles = FakeCollection([
            {"timeframe": "1m", "tradingDate": "2026-08-26"},
            {"timeframe": "1m", "tradingDate": "2026-08-27"},
            {"timeframe": "1m", "tradingDate": "2026-08-28"},
            {"timeframe": "5m", "tradingDate": "2026-08-25"},
        ])
        materializer = IntradayMaterializer(FakeCollection(), candles)

        result = materializer.prune_candles({"1m": 2, "5m": 3})

        self.assertEqual(result["byTimeframe"]["1m"]["cutoff"], "2026-08-27")
        self.assertEqual(result["byTimeframe"]["1m"]["deletedCount"], 1)
        self.assertEqual(result["byTimeframe"]["5m"]["reason"], "insufficient_trading_days")
        self.assertEqual(
            {(row["timeframe"], row["tradingDate"]) for row in candles.documents},
            {
                ("1m", "2026-08-27"),
                ("1m", "2026-08-28"),
                ("5m", "2026-08-25"),
            },
        )

    def test_lifecycle_settle_does_not_count_as_ohlcv_write(self):
        timestamp = 1_000_000
        close_at = datetime.fromtimestamp(bar_close_ms(timestamp, "5m") / 1000, timezone.utc)
        existing = {
            "symbol": "ABC",
            "timeframe": "5m",
            "timestamp": timestamp,
            "tradingDate": "2026-08-28",
            "open": 10,
            "high": 12,
            "low": 9,
            "close": 11,
            "volume": 5,
            "isForming": True,
            "barCloseAt": close_at,
            "updatedAt": datetime(2026, 8, 28, tzinfo=timezone.utc),
        }
        candles = FakeCollection([existing])
        materializer = IntradayMaterializer(FakeCollection(), candles)
        result = materializer._persist_candles(
            {"5m": [dict(existing)]},
            datetime(2026, 8, 28, 10, tzinfo=timezone.utc),
            close_at + timedelta(seconds=1),
            "2026-08-28",
        )

        self.assertEqual(result["examined"], 1)
        self.assertEqual(result["written"], 0)
        self.assertEqual(result["settled"], 1)
        self.assertEqual(result["changed"], {})
        self.assertFalse(candles.documents[0]["isForming"])

    def test_ohlcv_change_is_persisted_and_reported_by_pair(self):
        candles = FakeCollection()
        materializer = IntradayMaterializer(FakeCollection(), candles)
        candle = {
            "symbol": "ABC",
            "timeframe": "1m",
            "timestamp": 2_000_000,
            "tradingDate": "2026-08-28",
            "date": datetime.fromtimestamp(2_000_000 / 1000, timezone.utc),
            "open": 10,
            "high": 12,
            "low": 9,
            "close": 11,
            "volume": 5,
        }
        result = materializer._persist_candles(
            {"1m": [candle]},
            datetime(2026, 8, 28, tzinfo=timezone.utc),
            datetime(2026, 8, 28, tzinfo=timezone.utc),
            "2026-08-28",
        )

        self.assertEqual(result["written"], 1)
        self.assertEqual(result["settled"], 0)
        self.assertEqual(result["changed"]["1m"]["ABC"]["count"], 1)
        self.assertEqual(result["changed"]["1m"]["ABC"]["minTimestamp"], 2_000_000)
        self.assertEqual(candles.documents[0]["updatedAt"], datetime(2026, 8, 28, tzinfo=timezone.utc))


if __name__ == "__main__":
    unittest.main()
