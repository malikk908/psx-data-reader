import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from psx import market_watch_poller


class FakeWriteResult:
    upserted_count = 1
    modified_count = 0


class FakeCollection:
    def aggregate(self, pipeline):
        return []

    def bulk_write(self, operations, ordered=False):
        self.operations = operations
        return FakeWriteResult()


class FakeState:
    def __init__(self, generation=7):
        self.generation = generation
        self.published = []

    def publish_generation(self, scraped_at):
        self.generation += 1
        self.published.append(scraped_at)
        return {"generation": self.generation}


class MarketWatchPollerTests(unittest.TestCase):
    def test_successful_raw_batch_publishes_generation(self):
        scraped_at = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
        state = FakeState()
        quote = {
            "symbol": "ABC",
            "price": 10,
            "open": 10,
            "high": 10,
            "low": 10,
            "volume": 100,
        }

        with patch.object(market_watch_poller, "fetch_market_watch", return_value=([quote], scraped_at)):
            result = market_watch_poller.run_poll_cycle(FakeCollection(), object(), state)

        self.assertEqual(result["generation"], 8)
        self.assertEqual(state.published, [scraped_at])

    def test_partial_raw_write_does_not_publish_generation(self):
        class FailingCollection(FakeCollection):
            def bulk_write(self, operations, ordered=False):
                from pymongo.errors import BulkWriteError

                raise BulkWriteError({"nUpserted": 0, "nModified": 0, "writeErrors": []})

        scraped_at = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
        state = FakeState()
        quote = {
            "symbol": "ABC",
            "price": 10,
            "open": 10,
            "high": 10,
            "low": 10,
            "volume": 100,
        }

        with patch.object(market_watch_poller, "fetch_market_watch", return_value=([quote], scraped_at)):
            result = market_watch_poller.run_poll_cycle(FailingCollection(), object(), state)

        self.assertIsNone(result["generation"])
        self.assertEqual(state.published, [])

    def test_fetch_error_returns_full_summary(self):
        # main() logs every summary key; a missing one crashed the poller on each failed fetch.
        ok_quote = {"symbol": "ABC", "price": 10, "open": 10, "high": 10, "low": 10, "volume": 100}
        with patch.object(market_watch_poller, "fetch_market_watch",
                          return_value=([ok_quote], datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc))):
            ok = market_watch_poller.run_poll_cycle(FakeCollection(), object(), FakeState())

        with patch.object(market_watch_poller, "fetch_market_watch",
                          side_effect=ConnectionError("Remote end closed connection")):
            result = market_watch_poller.run_poll_cycle(FakeCollection(), object(), FakeState())

        self.assertTrue(result["fetch_error"])
        self.assertIsNone(result["generation"])
        self.assertEqual(set(result), set(ok))
        self.assertIsNone(ok["error"])
        self.assertEqual(result["error"]["code"], "PSX_FETCH_FAILED")
        self.assertIn("Remote end closed connection", result["error"]["message"])

    def test_write_failure_and_empty_response_report_an_error(self):
        class FailingCollection(FakeCollection):
            def bulk_write(self, operations, ordered=False):
                from pymongo.errors import BulkWriteError

                raise BulkWriteError({"nUpserted": 0, "nModified": 0, "writeErrors": [{"errmsg": "dup"}]})

        quote = {"symbol": "ABC", "price": 10, "open": 10, "high": 10, "low": 10, "volume": 100}
        scraped_at = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
        with patch.object(market_watch_poller, "fetch_market_watch", return_value=([quote], scraped_at)):
            failed = market_watch_poller.run_poll_cycle(FailingCollection(), object(), FakeState())
        self.assertEqual(failed["error"]["code"], "MONGO_WRITE_FAILED")

        with patch.object(market_watch_poller, "fetch_market_watch", return_value=([], scraped_at)):
            empty = market_watch_poller.run_poll_cycle(FakeCollection(), object(), FakeState())
        self.assertEqual(empty["error"]["code"], "PSX_EMPTY_RESPONSE")


if __name__ == "__main__":
    unittest.main()
