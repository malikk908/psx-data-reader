import json
import math
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from psx.technical_snapshot import build_technical_snapshot, round_to
from psx.intraday_technical_writer import (
    INTRADAY_LOOKBACK_CANDLES,
    INTRADAY_TIMEFRAMES,
    IntradayTechnicalWriter,
    primary_database_settings,
)


def _fixture_rows(fixture):
    start = datetime.fromisoformat(fixture["startDate"])
    rows = []
    for index in range(fixture["length"]):
        close = fixture["close"]["base"] + index * fixture["close"]["step"] + math.sin(index / fixture["close"]["sineDivisor"]) * fixture["close"]["sineAmplitude"]
        ohlc = fixture["ohlc"]
        rows.append({
            "date": start + timedelta(days=index),
            "open": close + ohlc["openOffset"],
            "high": close + ohlc["highOffset"] + (index % ohlc["highCycle"]) * ohlc["highCycleStep"],
            "low": close + ohlc["lowOffset"] - (index % ohlc["lowCycle"]) * ohlc["lowCycleStep"],
            "close": close,
            "volume": fixture["volume"]["base"] + index * fixture["volume"]["step"],
        })
    return rows


def _normalize(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    return value


class TechnicalSnapshotTests(unittest.TestCase):
    def test_backend_golden_fixture_matches_all_persisted_indicator_sections(self):
        fixture = json.loads((Path(__file__).parent / "fixtures" / "technical_snapshot_parity.json").read_text(encoding="utf-8"))
        snapshot = build_technical_snapshot(fixture["symbol"], _fixture_rows(fixture), datetime(2026, 3, 1, tzinfo=timezone.utc))
        actual = {key: snapshot[key] for key in fixture["expected"]}
        self.assertEqual(_normalize(actual), fixture["expected"], fixture["name"])

    def test_short_series_preserves_nested_shape_and_null_warmups(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [{"date": start + timedelta(minutes=index), "open": 10 + index, "high": 11 + index, "low": 9 + index, "close": 10 + index, "volume": 0} for index in range(13)]
        snapshot = build_technical_snapshot("SHORT", rows, start)

        self.assertIsNone(snapshot["movingAverages"]["ema20"])
        self.assertIsNone(snapshot["momentum"]["rsi14"])
        self.assertIsNone(snapshot["momentum"]["macd"]["signal"])
        self.assertIsNone(snapshot["volatility"]["atr14"])
        self.assertIsNone(snapshot["bands"]["bollinger"]["middle"])
        self.assertIsNone(snapshot["ratings"]["overall"]["score"])
        self.assertEqual(snapshot["volume"]["avgVolume10D"], 0)
        self.assertIsNone(snapshot["volumeFlow"]["cmf20D"])
        self.assertEqual(snapshot["pivots"]["classic"]["support"], [20, 19, 18])

        self.assertEqual(set(snapshot), {"symbol", "asOf", "computedAt", "snapshotClose", "snapshotVolume", "movingAverages", "clouds", "momentum", "bands", "volatility", "priceStructure", "volume", "volumeFlow", "trendStrength", "pivots", "ratings", "derived", "signals"})

    def test_rounding_matches_js_math_round_for_negative_half(self):
        self.assertEqual(round_to(1.23485), 1.2349)
        self.assertEqual(round_to(-1.23485), -1.2348)
        self.assertIsNone(round_to(float("nan")))


class _Cursor(list):
    def sort(self, fields):
        for field, direction in reversed(fields):
            super().sort(key=lambda row: row[field], reverse=direction < 0)
        return self

    def limit(self, count):
        return _Cursor(self[:count])


class _Collection:
    def __init__(self, rows=None):
        self.rows = list(rows or [])
        self.updated = None

    def find(self, query, projection=None):
        return _Cursor([row.copy() for row in self.rows if row["symbol"] == query["symbol"] and row["timeframe"] == query["timeframe"]])

    def update_one(self, selector, update, upsert=False):
        self.updated = (selector, update, upsert)

    def create_index(self, *args, **kwargs):
        return kwargs.get("name")


class WriterTests(unittest.TestCase):
    def test_writer_preserves_source_and_forming_bar_contract(self):
        base = datetime(2026, 8, 28, tzinfo=timezone.utc)
        candles = [{"symbol": "ABC", "timeframe": "5m", "timestamp": index, "date": base + timedelta(minutes=index), "open": 100 + index, "high": 101 + index, "low": 99 + index, "close": 100.5 + index, "volume": 10, "isForming": index == 2, "barCloseAt": base + timedelta(minutes=index + 5), "updatedAt": base + timedelta(minutes=index)} for index in range(3)]
        snapshots = _Collection(candles)
        writer = IntradayTechnicalWriter(snapshots, _Collection())
        result = writer.compute_and_save("abc", "5M", computed_at=base)

        self.assertEqual(result["symbol"], "ABC")
        self.assertEqual(result["timeframe"], "5m")
        self.assertEqual(result["barIsForming"], True)
        self.assertEqual(result["source"], {"baseCollection": "IntradayKline", "timeframe": "5m", "priceBasis": "raw", "lookbackCandles": 3, "firstCandleDate": base, "lastCandleDate": base + timedelta(minutes=2), "sourceUpdatedAt": base + timedelta(minutes=2)})

    def test_timeframes_and_backend_lookbacks_are_fixed(self):
        self.assertEqual(INTRADAY_TIMEFRAMES, ("1m", "5m", "15m", "30m", "1h", "4h"))
        self.assertEqual(INTRADAY_LOOKBACK_CANDLES, {"1m": 500, "5m": 500, "15m": 400, "30m": 400, "1h": 300, "4h": 300})

    def test_primary_database_configuration_is_separate_and_overridable(self):
        self.assertEqual(primary_database_settings({"FINHISAAB_PRIMARY_DB_MONGO_URI": "mongodb://primary", "FINHISAAB_PRIMARY_DB_NAME": "main"}), {"uri": "mongodb://primary", "db_name": "main"})


if __name__ == "__main__":
    unittest.main()
