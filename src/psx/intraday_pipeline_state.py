"""Durable coordination state for the stock intraday compute pipeline."""

from datetime import datetime, timedelta, timezone

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError


STATE_ID = "stock-intraday"
STATE_COLLECTION = "intraday_pipeline_state"


def utc_now():
    return datetime.now(timezone.utc)


class IntradayPipelineState:
    """Mongo-backed generation signal and single-worker lease."""

    def __init__(self, collection, state_id=STATE_ID):
        self.collection = collection
        self.state_id = state_id

    def ensure_indexes(self):
        # MongoDB creates the `_id` index automatically; this state collection
        # intentionally contains one small document.
        return None

    def publish_generation(self, scraped_at, written_at=None):
        """Publish only after the raw snapshot bulk write completed successfully."""
        written_at = written_at or utc_now()
        return self.collection.find_one_and_update(
            {"_id": self.state_id},
            {
                "$inc": {"generation": 1},
                "$set": {
                    "latestScrapedAt": scraped_at,
                    "latestRawWriteAt": written_at,
                    "updatedAt": written_at,
                },
                "$setOnInsert": {"completedGeneration": 0},
            },
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )

    def get(self):
        return self.collection.find_one({"_id": self.state_id})

    def claim_lease(self, owner, ttl_seconds=120, now=None):
        now = now or utc_now()
        lease_until = now + timedelta(seconds=ttl_seconds)
        state_exists = self.get() is not None
        try:
            return self.collection.find_one_and_update(
                {
                    "_id": self.state_id,
                    "$or": [
                        {"leaseUntil": {"$exists": False}},
                        {"leaseUntil": None},
                        {"leaseUntil": {"$lte": now}},
                        {"leaseOwner": owner},
                    ],
                },
                {
                    "$set": {
                        "leaseOwner": owner,
                        "leaseUntil": lease_until,
                        "updatedAt": now,
                    },
                    "$setOnInsert": {"generation": 0, "completedGeneration": 0},
                },
                upsert=not state_exists,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError:
            # Another worker created the state row between the existence probe
            # and this atomic claim; its lease remains authoritative.
            return None

    def renew_lease(self, owner, ttl_seconds=120, now=None):
        now = now or utc_now()
        lease_until = now + timedelta(seconds=ttl_seconds)
        return self.collection.find_one_and_update(
            {
                "_id": self.state_id,
                "leaseOwner": owner,
                "leaseUntil": {"$gt": now},
            },
            {"$set": {"leaseUntil": lease_until, "updatedAt": now}},
            return_document=ReturnDocument.AFTER,
        )

    def release_lease(self, owner, now=None):
        now = now or utc_now()
        return self.collection.find_one_and_update(
            {"_id": self.state_id, "leaseOwner": owner},
            {"$set": {"leaseOwner": None, "leaseUntil": None, "updatedAt": now}},
            return_document=ReturnDocument.AFTER,
        )

    def mark_completed(self, owner, generation, completed_at=None, require_active=False):
        completed_at = completed_at or utc_now()
        selector = {"_id": self.state_id, "leaseOwner": owner}
        if require_active:
            selector["leaseUntil"] = {"$gt": completed_at}
        return self.collection.find_one_and_update(
            selector,
            {
                "$set": {
                    "completedGeneration": generation,
                    "lastComputeAt": completed_at,
                    "updatedAt": completed_at,
                },
            },
            return_document=ReturnDocument.AFTER,
        )

    def mark_eod_completed(self, owner, trading_date, completed_at=None):
        """Record an EOD run only while this worker still owns an active lease."""
        completed_at = completed_at or utc_now()
        return self.collection.find_one_and_update(
            {
                "_id": self.state_id,
                "leaseOwner": owner,
                "leaseUntil": {"$gt": completed_at},
            },
            {
                "$set": {
                    "lastEodTradingDate": trading_date,
                    "lastEodAt": completed_at,
                    "updatedAt": completed_at,
                },
            },
            return_document=ReturnDocument.AFTER,
        )

    def record_failure(self, owner, error, failed_at=None):
        failed_at = failed_at or utc_now()
        return self.collection.find_one_and_update(
            {"_id": self.state_id, "leaseOwner": owner},
            {
                "$set": {
                    "lastError": str(error),
                    "lastErrorAt": failed_at,
                    "updatedAt": failed_at,
                },
            },
            return_document=ReturnDocument.AFTER,
        )
