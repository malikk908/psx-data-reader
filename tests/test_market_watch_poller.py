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

    def test_connection_errors_are_reported_not_raised(self):
        from pymongo.errors import AutoReconnect

        class DroppedReadCollection(FakeCollection):
            def aggregate(self, pipeline):
                raise AutoReconnect("mongo:27017: connection closed")

        class DroppedWriteCollection(FakeCollection):
            def bulk_write(self, operations, ordered=False):
                raise AutoReconnect("mongo:27017: connection closed")

        quote = {"symbol": "ABC", "price": 10, "open": 10, "high": 10, "low": 10, "volume": 100}
        scraped_at = datetime(2026, 8, 28, 10, 0, tzinfo=timezone.utc)
        for collection in (DroppedReadCollection(), DroppedWriteCollection()):
            state = FakeState()
            with patch.object(market_watch_poller, "fetch_market_watch", return_value=([quote], scraped_at)):
                result = market_watch_poller.run_poll_cycle(collection, object(), state)
            self.assertEqual(result["error"]["code"], "MONGO_UNAVAILABLE")
            self.assertIn("AutoReconnect", result["error"]["message"])
            self.assertFalse(result["fetch_error"])
            self.assertEqual(state.published, [])


class FakeHealth:
    def __init__(self):
        self.attempts = 0
        self.failures = []

    def attempt(self):
        self.attempts += 1

    def failure(self, code, message):
        self.failures.append((code, message))


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class WaitForMongoTests(unittest.TestCase):
    def test_retries_with_backoff_until_mongo_answers(self):
        health, clock = FakeHealth(), FakeClock()
        answers = iter(["ServerSelectionTimeoutError: down"] * 3 + [None])
        with patch.object(market_watch_poller, "mongo_connectivity_error", side_effect=lambda *a: next(answers)):
            market_watch_poller.wait_for_mongo(
                health, "mongodb://x", "db", give_up_after_s=600,
                sleep=clock.sleep, monotonic=clock.monotonic,
            )

        self.assertEqual(clock.sleeps, [5, 10, 20])
        self.assertEqual(health.attempts, 4)
        self.assertEqual([code for code, _ in health.failures], ["MONGO_UNAVAILABLE"] * 3)
        self.assertIn("ServerSelectionTimeoutError", health.failures[0][1])

    def test_gives_up_by_raising_instead_of_returning(self):
        # Returning ended the process with exit code 0, so an "on failure"
        # restart policy never brought the poller back.
        health, clock = FakeHealth(), FakeClock()
        with patch.object(market_watch_poller, "mongo_connectivity_error", return_value="AutoReconnect: down"):
            with self.assertRaises(market_watch_poller.MongoUnavailableAtStartup) as raised:
                market_watch_poller.wait_for_mongo(
                    health, "mongodb://x", "db", give_up_after_s=120,
                    sleep=clock.sleep, monotonic=clock.monotonic,
                )

        self.assertEqual(sum(clock.sleeps), 120)
        self.assertLessEqual(max(clock.sleeps), 60)
        self.assertIn("AutoReconnect: down", str(raised.exception))

    def test_unreachable_mongo_at_startup_is_reported_fatal_and_exits_non_zero(self):
        from psx.feed_health import FeedHealth

        health = FeedHealth("psx-intraday")
        with patch.dict("os.environ", {"INTRADAY_MONGO_STARTUP_WAIT_MIN": "0"}), \
                patch.object(market_watch_poller, "mongo_connectivity_error", return_value="AutoReconnect: down"), \
                patch.object(health, "report") as report:
            with self.assertRaises(market_watch_poller.MongoUnavailableAtStartup):
                health.run(lambda: market_watch_poller.poll_forever(health))

        self.assertEqual([c.args[0] for c in report.call_args_list], ["started", "fatal"])
        self.assertIn("MongoDB unreachable at startup", report.call_args_list[1].kwargs["message"])
        self.assertEqual(health.snapshot()["last_error"]["code"], "MONGO_UNAVAILABLE")


if __name__ == "__main__":
    unittest.main()
