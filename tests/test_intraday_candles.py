import unittest
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from psx.intraday_candles import (
    PSX_FEED_DELAY_MS,
    bar_close_ms,
    bar_lifecycle,
    build_higher_timeframe_candles,
    build_one_minute_candles,
    candle_differs,
    lifecycle_differs,
)


PKT = timezone(timedelta(hours=5))


def at_pkt(text):
    return datetime.fromisoformat(text).replace(tzinfo=PKT)


def snapshot(text, **fields):
    scraped_at = at_pkt(text)
    return {
        "symbol": "ABC",
        "scraped_at": scraped_at,
        "scraped_at_minute": scraped_at,
        **fields,
    }


class CandleBuilderTests(unittest.TestCase):
    def test_node_parity_fixture_for_one_minute_builder(self):
        fixture_path = Path(__file__).parent / "fixtures" / "candle_parity.json"
        fixtures = json.loads(fixture_path.read_text(encoding="utf-8"))

        for fixture in fixtures:
            snapshots = []
            for raw_snapshot in fixture["snapshots"]:
                snapshot_data = dict(raw_snapshot)
                snapshot_data["scraped_at"] = datetime.fromisoformat(snapshot_data["scraped_at"])
                snapshot_data["scraped_at_minute"] = datetime.fromisoformat(snapshot_data["scraped_at_minute"])
                snapshots.append(snapshot_data)

            actual = [
                {field: candle[field] for field in fixture["expected"][0] if field not in {"date"}}
                for candle in build_one_minute_candles(snapshots)
            ]
            self.assertEqual(actual, fixture["expected"], fixture["name"])

            higher = fixture["higher"]
            actual_higher = [
                {field: candle[field] for field in higher["expected"][0]}
                for candle in build_higher_timeframe_candles(
                    build_one_minute_candles(snapshots),
                    higher["timeframe"],
                    higher["minutes"],
                )
            ]
            self.assertEqual(actual_higher, higher["expected"], fixture["name"])

    def test_opening_bar_uses_session_fields_and_delayed_timestamp(self):
        candles = build_one_minute_candles([
            snapshot(
                "2026-06-20T09:31:00",
                price=101,
                open=100,
                high=102,
                low=99,
                volume=5000,
                volume_delta=None,
            )
        ])

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0]["open"], 100.0)
        self.assertEqual(candles[0]["high"], 102.0)
        self.assertEqual(candles[0]["low"], 99.0)
        self.assertEqual(candles[0]["close"], 101.0)
        self.assertEqual(candles[0]["volume"], 5000.0)
        self.assertEqual(
            candles[0]["timestamp"],
            int(at_pkt("2026-06-20T09:31:00").timestamp() * 1000) - PSX_FEED_DELAY_MS,
        )

    def test_subsequent_bar_chains_open_and_uses_volume_delta(self):
        candles = build_one_minute_candles([
            snapshot("2026-06-20T09:31:00", price=100, open=100, high=100, low=100, volume=1000, volume_delta=None),
            snapshot("2026-06-20T09:32:00", price=102, open=100, high=102, low=100, volume=1500, volume_delta=500),
        ])

        self.assertEqual(candles[1]["open"], 100.0)
        self.assertEqual(candles[1]["close"], 102.0)
        self.assertEqual(candles[1]["volume"], 500.0)

    def test_new_session_extreme_is_attributed_to_current_bar(self):
        candles = build_one_minute_candles([
            snapshot("2026-06-20T09:31:00", price=100, open=100, high=100, low=100, volume=1000, volume_delta=None),
            snapshot("2026-06-20T09:32:00", price=99, open=100, high=105, low=99, volume=1200, volume_delta=200),
        ])

        self.assertEqual(candles[1]["high"], 105.0)
        self.assertEqual(candles[1]["low"], 99.0)

    def test_higher_timeframe_aggregation_is_utc_aligned_and_lossless(self):
        one_minute = [
            {"symbol": "ABC", "timeframe": "1m", "timestamp": 1000, "tradingDate": "2026-06-20", "date": datetime.now(timezone.utc), "open": 10, "high": 12, "low": 9, "close": 11, "volume": 5},
            {"symbol": "ABC", "timeframe": "1m", "timestamp": 2000, "tradingDate": "2026-06-20", "date": datetime.now(timezone.utc), "open": 11, "high": 14, "low": 10, "close": 13, "volume": 7},
        ]
        candles = build_higher_timeframe_candles(one_minute, "5m", 5)

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0]["timestamp"], 0)
        self.assertEqual(candles[0]["open"], 10)
        self.assertEqual(candles[0]["high"], 14)
        self.assertEqual(candles[0]["low"], 9)
        self.assertEqual(candles[0]["close"], 13)
        self.assertEqual(candles[0]["volume"], 12)


class LifecycleTests(unittest.TestCase):
    def test_forming_boundary_is_closed_at_close_instant(self):
        timestamp = 1_000_000
        close = bar_close_ms(timestamp, "5m")
        self.assertTrue(bar_lifecycle(timestamp, "5m", datetime.fromtimestamp((close - 1) / 1000, timezone.utc))["isForming"])
        self.assertFalse(bar_lifecycle(timestamp, "5m", datetime.fromtimestamp(close / 1000, timezone.utc))["isForming"])

    def test_lifecycle_only_settles_once(self):
        timestamp = 1_000_000
        close = datetime.fromtimestamp(bar_close_ms(timestamp, "5m") / 1000, timezone.utc)
        closed = {"isForming": False, "barCloseAt": close}
        forming = {"isForming": True, "barCloseAt": close}
        self.assertFalse(lifecycle_differs(closed, closed))
        self.assertTrue(lifecycle_differs(forming, closed))
        self.assertTrue(lifecycle_differs({"open": 1}, closed))

    def test_change_detection_ignores_lifecycle_and_metadata(self):
        incoming = {"open": 10, "high": 12, "low": 9, "close": 11, "volume": 5}
        existing = {**incoming, "isForming": True, "updatedAt": datetime.now(timezone.utc)}
        self.assertFalse(candle_differs(incoming, existing))
        self.assertTrue(candle_differs({**incoming, "close": 12}, existing))


if __name__ == "__main__":
    unittest.main()
