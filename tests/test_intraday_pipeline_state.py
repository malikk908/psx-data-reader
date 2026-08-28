import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

from pymongo.errors import DuplicateKeyError

from psx.intraday_pipeline_state import IntradayPipelineState


class PipelineStateTests(unittest.TestCase):
    def test_publish_generation_increments_and_records_raw_watermarks(self):
        collection = Mock()
        returned = {"_id": "stock-intraday", "generation": 12}
        collection.find_one_and_update.return_value = returned
        state = IntradayPipelineState(collection)
        scraped_at = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)
        written_at = datetime(2026, 8, 28, 10, 1, tzinfo=timezone.utc)

        self.assertIs(state.publish_generation(scraped_at, written_at), returned)
        update = collection.find_one_and_update.call_args.args[1]
        self.assertEqual(update["$inc"], {"generation": 1})
        self.assertEqual(update["$set"]["latestScrapedAt"], scraped_at)
        self.assertTrue(collection.find_one_and_update.call_args.kwargs["upsert"])

    def test_existing_active_lease_is_not_upserted(self):
        collection = Mock()
        collection.find_one.return_value = {"_id": "stock-intraday", "leaseOwner": "other"}
        collection.find_one_and_update.return_value = None
        state = IntradayPipelineState(collection)
        now = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)

        self.assertIsNone(state.claim_lease("worker", now=now))
        self.assertFalse(collection.find_one_and_update.call_args.kwargs["upsert"])

    def test_racing_state_creation_does_not_raise(self):
        collection = Mock()
        collection.find_one.return_value = None
        collection.find_one_and_update.side_effect = DuplicateKeyError({})
        state = IntradayPipelineState(collection)

        self.assertIsNone(state.claim_lease("worker"))

    def test_release_and_completion_are_owner_scoped(self):
        collection = Mock()
        collection.find_one_and_update.return_value = {"leaseOwner": "worker"}
        state = IntradayPipelineState(collection)
        completed_at = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)

        state.mark_completed("worker", 5, completed_at)
        completed_filter = collection.find_one_and_update.call_args.args[0]
        self.assertEqual(completed_filter, {"_id": "stock-intraday", "leaseOwner": "worker"})
        self.assertEqual(
            collection.find_one_and_update.call_args.args[1]["$set"]["completedGeneration"],
            5,
        )

        state.release_lease("worker", completed_at + timedelta(seconds=1))
        release_filter = collection.find_one_and_update.call_args.args[0]
        self.assertEqual(release_filter["leaseOwner"], "worker")

    def test_renewal_requires_an_unexpired_lease(self):
        collection = Mock()
        collection.find_one_and_update.return_value = None
        state = IntradayPipelineState(collection)
        now = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)

        self.assertIsNone(state.renew_lease("worker", now=now))
        self.assertEqual(
            collection.find_one_and_update.call_args.args[0],
            {
                "_id": "stock-intraday",
                "leaseOwner": "worker",
                "leaseUntil": {"$gt": now},
            },
        )


if __name__ == "__main__":
    unittest.main()
