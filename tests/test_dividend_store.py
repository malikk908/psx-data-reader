import unittest
from datetime import datetime
from unittest.mock import patch

import pandas as pd

try:
    import mongomock
except ImportError:  # pragma: no cover - dev dependency, see requirements-dev.txt
    mongomock = None

from psx import dividend_store

NOW = datetime(2026, 10, 20, 12, 0, 0)
DIVIDENDS = "dividendannouncements"
BONUSES = "bonusannouncements"
ARCHIVE = "dividendannouncements_archive"


def scraped(symbol, percent, x_date, sector="FOOD"):
    return {
        "symbol": symbol,
        "name": symbol,
        "dividend": percent,
        "bonus": "",
        "right": "",
        "x_date": x_date,
        "sector": sector,
        "announcement_type": ["dividend"],
    }


class FaceValueAtTests(unittest.TestCase):
    # DLL: face value is Rs 1 now after a 10-for-1 split (900%) on 2026-06-05.
    face_values = {"DLL": 1.0, "SOYASUP": 2.0, "PLAIN": 10.0}
    splits = {"DLL": [(datetime(2026, 6, 5), 10.0)]}

    def test_dividend_before_a_split_keeps_the_old_face_value(self):
        self.assertEqual(
            dividend_store.face_value_at("DLL", datetime(2026, 5, 7), self.face_values, self.splits, NOW), 10.0)

    def test_dividend_after_a_split_uses_the_new_face_value(self):
        self.assertEqual(
            dividend_store.face_value_at("DLL", datetime(2026, 9, 8), self.face_values, self.splits, NOW), 1.0)

    def test_split_that_has_not_happened_yet_is_ignored(self):
        splits = {"DLL": [(datetime(2027, 1, 1), 10.0)]}
        self.assertEqual(
            dividend_store.face_value_at("DLL", datetime(2026, 9, 8), self.face_values, splits, NOW), 1.0)

    def test_stock_without_splits_uses_its_face_value(self):
        self.assertEqual(
            dividend_store.face_value_at("SOYASUP", datetime(2026, 10, 16), self.face_values, self.splits, NOW), 2.0)

    def test_unknown_symbol_has_no_face_value(self):
        self.assertIsNone(dividend_store.face_value_at("NOPE", datetime(2026, 10, 16), self.face_values, {}, NOW))


class ProcessAnnouncementsTests(unittest.TestCase):
    def test_amount_comes_from_the_stock_face_value(self):
        df = pd.DataFrame([scraped("SOYASUP", 25.0, datetime(2026, 10, 16))])

        dividends, _ = dividend_store.process_announcements(df, {"SOYASUP": 2.0}, {}, NOW)

        self.assertEqual(len(dividends), 1)
        self.assertAlmostEqual(dividends[0]["amountPerShare"], 0.5)
        self.assertEqual(dividends[0]["faceValue"], 2.0)
        self.assertEqual(dividends[0]["metadata"]["dividendPercent"], 25.0)
        self.assertEqual(dividends[0]["metadata"]["source"], "scstrade")

    def test_symbol_missing_from_stocks_is_skipped_not_priced_at_rs_10(self):
        df = pd.DataFrame([scraped("NEWCO", 25.0, datetime(2026, 10, 16))])

        dividends, _ = dividend_store.process_announcements(df, {"SOYASUP": 2.0}, {}, NOW)

        self.assertEqual(dividends, [])


@unittest.skipIf(mongomock is None, "mongomock is not installed")
class ArchiveAndCorrectionTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient().db

    def psx(self, symbol, ex_date, amount, source="PSX #28404 - Financial results"):
        return self.db[DIVIDENDS].insert_one({
            "symbol": symbol, "amountPerShare": amount, "exDate": ex_date, "status": "ANNOUNCED",
            "metadata": {"source": source},
        }).inserted_id

    def incoming(self, symbol, ex_date, amount):
        return {"symbol": symbol, "exDate": ex_date, "amountPerShare": amount, "faceValue": 2.0,
                "metadata": {"source": "scstrade", "dividendPercent": 25.0}}

    def test_nearby_psx_announcement_is_archived_in_favour_of_scstrade(self):
        psx_id = self.psx("SOYASUP", datetime(2026, 10, 19), 0.5)

        archived, blocked = dividend_store.archive_superseded_announcements(
            self.db, [self.incoming("SOYASUP", datetime(2026, 10, 16), 0.5)], DIVIDENDS, ARCHIVE, NOW)

        self.assertEqual((archived, blocked), (1, set()))
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": psx_id}), 0)
        stored = self.db[ARCHIVE].find_one({"_id": psx_id})
        self.assertEqual(stored["amountPerShare"], 0.5)
        self.assertEqual(stored["archiveReason"], "superseded_by_scstrade")
        self.assertEqual(stored["supersededBy"]["exDate"], datetime(2026, 10, 16))

    def test_announcements_from_other_sources_or_far_away_are_kept(self):
        manual = self.psx("SOYASUP", datetime(2026, 10, 17), 0.5, source="manual")
        legacy = self.db[DIVIDENDS].insert_one(
            {"symbol": "SOYASUP", "amountPerShare": 1, "exDate": datetime(2026, 10, 18)}).inserted_id
        far = self.psx("SOYASUP", datetime(2026, 12, 1), 0.5)
        other_symbol = self.psx("OTHER", datetime(2026, 10, 16), 1)

        archived, _ = dividend_store.archive_superseded_announcements(
            self.db, [self.incoming("SOYASUP", datetime(2026, 10, 16), 0.5)], DIVIDENDS, ARCHIVE, NOW)

        self.assertEqual(archived, 0)
        for kept in (manual, legacy, far, other_symbol):
            self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": kept}), 1)
        self.assertEqual(self.db[ARCHIVE].count_documents({}), 0)

    def test_announcement_with_entitlements_is_not_archived_and_blocks_the_insert(self):
        psx_id = self.psx("SOYASUP", datetime(2026, 10, 19), 0.5)
        self.db["dividendentitlements"].insert_one({"announcementId": psx_id, "isActive": True})

        archived, blocked = dividend_store.archive_superseded_announcements(
            self.db, [self.incoming("SOYASUP", datetime(2026, 10, 16), 0.5)], DIVIDENDS, ARCHIVE, NOW)

        self.assertEqual(archived, 0)
        self.assertEqual(blocked, {("SOYASUP", datetime(2026, 10, 16))})
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": psx_id}), 1)

    def test_existing_announcement_is_corrected_when_it_has_no_entitlements(self):
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 2.5, "faceValue": 10, "exDate": datetime(2026, 10, 22),
            "status": "ANNOUNCED", "metadata": {"source": "scstrade"},
        }).inserted_id

        updated = dividend_store.correct_existing_dividends(
            self.db, [self.incoming("SOYASUP", datetime(2026, 10, 22), 0.5)], DIVIDENDS, NOW)

        stored = self.db[DIVIDENDS].find_one({"_id": doc_id})
        self.assertEqual(updated, 1)
        self.assertEqual((stored["amountPerShare"], stored["faceValue"]), (0.5, 2.0))
        self.assertEqual(stored["metadata"]["dividendPercent"], 25.0)

    def test_existing_announcement_with_entitlements_is_left_alone(self):
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 2.5, "faceValue": 10, "exDate": datetime(2026, 10, 16),
            "status": "ANNOUNCED", "metadata": {"source": "scstrade"},
        }).inserted_id
        self.db["dividendentitlements"].insert_one({"announcementId": doc_id, "isActive": True})

        updated = dividend_store.correct_existing_dividends(
            self.db, [self.incoming("SOYASUP", datetime(2026, 10, 16), 0.5)], DIVIDENDS, NOW)

        self.assertEqual(updated, 0)
        self.assertEqual(self.db[DIVIDENDS].find_one({"_id": doc_id})["amountPerShare"], 2.5)

    def test_record_without_a_face_value_field_is_left_alone_when_its_amount_is_right(self):
        # UBL, Rs 8 = 160% of Rs 5 after its split. Saved before face values were tracked.
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "UBL", "amountPerShare": 8, "exDate": datetime(2026, 10, 23),
            "status": "ANNOUNCED", "metadata": {"source": "scstrade"},
        }).inserted_id
        incoming = {"symbol": "UBL", "exDate": datetime(2026, 10, 23), "amountPerShare": 8.0, "faceValue": 5.0,
                    "metadata": {"source": "scstrade", "dividendPercent": 160.0}}

        updated = dividend_store.correct_existing_dividends(self.db, [incoming], DIVIDENDS, NOW)

        self.assertEqual(updated, 0)
        self.assertNotIn("faceValue", self.db[DIVIDENDS].find_one({"_id": doc_id}))

    def test_records_older_than_the_lookback_are_never_changed(self):
        # Dividend from before a split, recomputed here as if it were new: it must stay.
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "UBL", "amountPerShare": 8, "exDate": datetime(2025, 10, 23),
            "status": "ANNOUNCED", "metadata": {"source": "scstrade"},
        }).inserted_id
        incoming = {"symbol": "UBL", "exDate": datetime(2025, 10, 23), "amountPerShare": 4.0, "faceValue": 5.0,
                    "metadata": {"source": "scstrade", "dividendPercent": 80.0}}

        updated = dividend_store.correct_existing_dividends(self.db, [incoming], DIVIDENDS, NOW)

        self.assertEqual(updated, 0)
        self.assertEqual(self.db[DIVIDENDS].find_one({"_id": doc_id})["amountPerShare"], 8)


@unittest.skipIf(mongomock is None, "mongomock is not installed")
class SaveAnnouncementsTests(unittest.TestCase):
    def setUp(self):
        self.db = mongomock.MongoClient().db
        self.db["stocks"].insert_many([
            {"symbol": "SOYASUP", "faceValue": 2},
            {"symbol": "NML"},
        ])
        patcher = patch.object(dividend_store, "connect_to_mongodb", return_value=self.db)
        patcher.start()
        self.addCleanup(patcher.stop)

    def save(self, rows):
        return dividend_store.save_announcements_to_mongodb(
            pd.DataFrame(rows), "uri", "db", DIVIDENDS, BONUSES, ARCHIVE)

    def test_scstrade_dividend_replaces_the_psx_duplicate_with_the_right_amount(self):
        psx_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 0.5, "exDate": datetime(2999, 10, 19), "status": "ANNOUNCED",
            "metadata": {"source": "PSX #28404"},
        }).inserted_id

        ok, message = self.save([scraped("SOYASUP", 25.0, datetime(2999, 10, 16))])

        self.assertTrue(ok, message)
        remaining = list(self.db[DIVIDENDS].find({}))
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["metadata"]["source"], "scstrade")
        self.assertAlmostEqual(remaining[0]["amountPerShare"], 0.5)
        self.assertEqual(remaining[0]["faceValue"], 2.0)
        self.assertEqual(self.db[ARCHIVE].count_documents({"_id": psx_id}), 1)

    def test_blocked_duplicate_is_not_inserted(self):
        psx_id = self.db[DIVIDENDS].insert_one({
            "symbol": "NML", "amountPerShare": 2, "exDate": datetime(2999, 10, 21), "status": "ANNOUNCED",
            "metadata": {"source": "PSX #28394"},
        }).inserted_id
        self.db["dividendentitlements"].insert_one({"announcementId": psx_id, "isActive": True})

        ok, _ = self.save([scraped("NML", 20.0, datetime(2999, 10, 20))])

        self.assertTrue(ok)
        self.assertEqual(self.db[DIVIDENDS].count_documents({}), 1)
        self.assertEqual(self.db[DIVIDENDS].count_documents({"metadata.source": "scstrade"}), 0)

    def test_dividend_for_unknown_stock_is_not_saved(self):
        ok, _ = self.save([scraped("NEWCO", 25.0, datetime(2999, 10, 16))])

        self.assertTrue(ok)
        self.assertEqual(self.db[DIVIDENDS].count_documents({}), 0)

    def test_split_stock_dividends_use_the_face_value_of_their_ex_date(self):
        self.db["stocks"].insert_one({"symbol": "DLL", "faceValue": 1})
        self.db[BONUSES].insert_one({
            "symbol": "DLL", "bonusType": "STOCK_SPLIT", "bonusPercentage": 900,
            "exDate": datetime(2026, 6, 5), "status": "CONFIRMED",
        })

        ok, _ = self.save([
            scraped("DLL", 30.0, datetime(2026, 5, 7)),
            scraped("DLL", 30.0, datetime(2026, 9, 8)),
        ])

        self.assertTrue(ok)
        by_date = {d["exDate"]: d for d in self.db[DIVIDENDS].find({})}
        self.assertAlmostEqual(by_date[datetime(2026, 5, 7)]["amountPerShare"], 3.0)
        self.assertAlmostEqual(by_date[datetime(2026, 9, 8)]["amountPerShare"], 0.3)

    def test_listed_scstrade_dividend_with_entitlements_is_kept_when_it_drops_off_the_feed(self):
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 0.5, "exDate": datetime(2999, 10, 16), "status": "ANNOUNCED",
            "metadata": {"source": "scstrade"},
        }).inserted_id
        self.db["dividendentitlements"].insert_one({"announcementId": doc_id, "isActive": True})

        ok, _ = self.save([scraped("NML", 20.0, datetime(2999, 10, 20))])

        self.assertTrue(ok)
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": doc_id}), 1)

    def test_empty_stocks_collection_aborts_without_deleting_anything(self):
        # Wrong database: no stocks, so nothing can be priced.
        self.db["stocks"].delete_many({})
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 0.5, "exDate": datetime(2999, 10, 16), "status": "ANNOUNCED",
            "metadata": {"source": "scstrade"},
        }).inserted_id

        ok, message = self.save([scraped("SOYASUP", 25.0, datetime(2999, 10, 16))])

        self.assertFalse(ok)
        self.assertIn("No face values", message)
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": doc_id}), 1)

    def test_future_dividend_for_symbol_missing_from_stocks_is_not_deleted_as_orphan(self):
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "DELISTED", "amountPerShare": 1, "exDate": datetime(2999, 10, 16), "status": "ANNOUNCED",
            "metadata": {"source": "scstrade"},
        }).inserted_id

        ok, _ = self.save([scraped("DELISTED", 10.0, datetime(2999, 10, 16))])

        self.assertTrue(ok)
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": doc_id}), 1)

    def test_unlisted_future_dividend_for_a_known_stock_is_still_deleted(self):
        doc_id = self.db[DIVIDENDS].insert_one({
            "symbol": "SOYASUP", "amountPerShare": 0.5, "exDate": datetime(2999, 10, 16), "status": "ANNOUNCED",
            "metadata": {"source": "scstrade"},
        }).inserted_id

        ok, _ = self.save([scraped("NML", 20.0, datetime(2999, 10, 20))])

        self.assertTrue(ok)
        self.assertEqual(self.db[DIVIDENDS].count_documents({"_id": doc_id}), 0)


if __name__ == "__main__":
    unittest.main()
