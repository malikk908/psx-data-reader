"""
MongoDB storage functionality for PSX dividend and bonus announcements.
Aligns with Mongoose schemas DividendAnnouncement and BonusAnnouncement.

scstrade is the trusted source for ex-dates. The backend also creates
announcements from PSX filings ("PSX #..." source), and the two usually
disagree by a day or a weekend, so the same dividend would be credited twice.
When scstrade has a dividend, any PSX-sourced announcement for the same symbol
within ARCHIVE_WINDOW_DAYS is moved to the archive collection.
"""

import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCSTRADE_SOURCE = "scstrade"
DEFAULT_FACE_VALUE = 10.0  # Stock.faceValue is not stored when it is 10
ENTITLEMENT_COLLECTION = "dividendentitlements"
DEFAULT_ARCHIVE_COLLECTION = "dividendannouncements_archive"

# A PSX announcement this close to a scstrade one is the same dividend.
ARCHIVE_WINDOW_DAYS = 7
# Only announcements this recent are considered for archiving or correction.
RECENT_LOOKBACK_DAYS = 60


def connect_to_mongodb(connection_string, db_name):
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise


def create_indexes(db, dividend_collection_name, bonus_collection_name):
    try:
        div_coll = db[dividend_collection_name]
        bonus_coll = db[bonus_collection_name]

        # Dividend indexes
        div_coll.create_index([("symbol", pymongo.ASCENDING), ("exDate", pymongo.ASCENDING)], unique=True, name="symbol_exdate_unique")
        div_coll.create_index([("payDate", pymongo.ASCENDING)], name="paydate_asc")
        div_coll.create_index([("status", pymongo.ASCENDING)], name="status_asc")

        # Bonus indexes
        bonus_coll.create_index([("symbol", pymongo.ASCENDING), ("exDate", pymongo.ASCENDING)], name="symbol_exdate")
        bonus_coll.create_index([("exDate", pymongo.ASCENDING)], name="exdate_asc")
        bonus_coll.create_index([("status", pymongo.ASCENDING)], name="status_asc")

        logger.info("Successfully created indexes")

    except Exception as e:
        logger.warning(f"Index creation warning (may already exist): {str(e)}")


def load_face_values(db):
    """
    Current face value per symbol, read from the stocks collection.

    A stock without a stored faceValue has the default Rs 10. A symbol that is
    not in stocks at all is left out, so its dividends are skipped instead of
    being priced at an assumed face value.
    """
    face_values = {}
    for stock in db['stocks'].find({}, {'symbol': 1, 'faceValue': 1, '_id': 0}):
        symbol = stock.get('symbol')
        if not symbol:
            continue
        face_value = stock.get('faceValue')
        face_values[symbol] = float(face_value) if face_value and face_value > 0 else DEFAULT_FACE_VALUE
    return face_values


def load_splits(db, bonus_collection_name):
    """
    Stock splits per symbol as (exDate, factor), where a 900% split has factor 10.
    """
    splits = {}
    for split in db[bonus_collection_name].find({"bonusType": "STOCK_SPLIT"}):
        percentage = split.get("bonusPercentage")
        if not split.get("symbol") or not split.get("exDate") or not percentage or percentage <= 0:
            continue
        splits.setdefault(split["symbol"], []).append((split["exDate"], 1 + percentage / 100.0))
    return splits


def face_value_at(symbol, ex_date, face_values, splits=None, now=None):
    """
    Face value in force on ex_date, or None when the stock is unknown.

    stocks.faceValue already reflects every split that has taken effect, so a
    dividend dated before such a split was quoted on the larger face value:
    undo each split whose ex-date is after the dividend and not in the future.
    """
    face_value = face_values.get(symbol)
    if face_value is None:
        return None

    now = now or datetime.now()
    for split_ex_date, factor in (splits or {}).get(symbol, []):
        if ex_date < split_ex_date <= now:
            face_value *= factor
    return round(face_value, 6)


def process_announcements(announcements_df, face_values, splits=None, now=None):
    if announcements_df.empty:
        return [], []

    dividends = []
    bonuses = []
    unknown_symbols = set()

    now = now or datetime.now()

    for _, row in announcements_df.iterrows():
        # Get actual python datetime
        x_date = row['x_date']
        if hasattr(x_date, 'to_pydatetime'):
            x_date = x_date.to_pydatetime()

        symbol = row.get('symbol', '').strip()
        if not symbol:
            continue

        base_metadata = {
            "source": SCSTRADE_SOURCE,
            "notes": f"Sector: {row.get('sector', '')}"
        }

        # Process Dividend
        if "dividend" in row.get('announcement_type', []):
            try:
                percent_amount = float(row.get('dividend', 0))
                if percent_amount > 0:
                    face_value = face_value_at(symbol, x_date, face_values, splits, now)
                    if face_value is None:
                        # scstrade quotes a percentage of face value, so without the
                        # stock's face value the amount per share is unknown.
                        unknown_symbols.add(symbol)
                    else:
                        actual_amount = (percent_amount * face_value) / 100.0

                        dividends.append({
                            "symbol": symbol,
                            "amountPerShare": actual_amount,
                            "faceValue": face_value,
                            "exDate": x_date,
                            "payDate": x_date, # PSX only gives xDate, use it for required payDate
                            "status": "ANNOUNCED",
                            "metadata": {**base_metadata, "dividendPercent": percent_amount},
                            "createdAt": now,
                            "updatedAt": now
                        })
            except Exception as e:
                logger.warning(f"Could not parse dividend amount for {symbol}: {e}")

        # Process Bonus
        if "bonus" in row.get('announcement_type', []) and row.get('bonus'):
            try:
                bonus_str = str(row['bonus']).replace('%', '').strip()
                if bonus_str:
                    amount = float(bonus_str)
                    if amount > 0:
                        bonuses.append({
                            "symbol": symbol,
                            "bonusPercentage": amount,
                            "bonusType": "BONUS_SHARES",
                            "exDate": x_date,
                            "status": "ANNOUNCED",
                            "metadata": base_metadata,
                            "createdAt": now,
                            "updatedAt": now
                        })
            except Exception as e:
                logger.warning(f"Could not parse bonus amount for {symbol}: {e}")

    if unknown_symbols:
        logger.warning(
            f"Skipped dividends for {len(unknown_symbols)} symbol(s) missing from stocks "
            f"(face value unknown): {', '.join(sorted(unknown_symbols))}"
        )

    logger.info(f"Parsed {len(dividends)} dividends and {len(bonuses)} bonuses")
    return dividends, bonuses


def _has_entitlements(db, announcement_id):
    return db[ENTITLEMENT_COLLECTION].count_documents(
        {"announcementId": announcement_id, "isActive": {"$ne": False}}, limit=1
    ) > 0


def archive_superseded_announcements(db, dividends, dividend_collection_name,
                                     archive_collection_name=DEFAULT_ARCHIVE_COLLECTION,
                                     now=None):
    """
    Move PSX-sourced announcements that duplicate a scstrade dividend into the
    archive collection, so each dividend exists once, with scstrade's ex-date.

    An announcement that already has entitlements is left alone, because
    removing it would orphan them. Such a case needs a repair on the backend,
    and the scstrade dividend is not inserted meanwhile so the two never
    both produce entitlements.

    Returns (archived_count, blocked_keys), where blocked_keys holds the
    (symbol, exDate) of scstrade dividends that must not be inserted yet.
    """
    now = now or datetime.now()
    if not dividends:
        return 0, set()

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    window = timedelta(days=ARCHIVE_WINDOW_DAYS)
    cutoff = today_start - timedelta(days=RECENT_LOOKBACK_DAYS)

    div_coll = db[dividend_collection_name]
    candidates_by_symbol = {}
    for doc in div_coll.find({
        "metadata.source": {"$regex": "^PSX"},
        "exDate": {"$gte": cutoff - window},
    }):
        candidates_by_symbol.setdefault(doc.get("symbol"), []).append(doc)

    archived = 0
    blocked_keys = set()
    handled = set()

    for dividend in dividends:
        for candidate in candidates_by_symbol.get(dividend["symbol"], []):
            if candidate["_id"] in handled:
                continue
            if abs(candidate["exDate"] - dividend["exDate"]) > window:
                continue

            if _has_entitlements(db, candidate["_id"]):
                logger.warning(
                    f"{dividend['symbol']}: PSX announcement {candidate['_id']} "
                    f"(ex {candidate['exDate']:%Y-%m-%d}) duplicates scstrade "
                    f"(ex {dividend['exDate']:%Y-%m-%d}) but has entitlements. "
                    f"Not archived and scstrade not inserted; repair on the backend."
                )
                blocked_keys.add((dividend["symbol"], dividend["exDate"]))
                handled.add(candidate["_id"])
                continue

            archived_doc = {
                **candidate,
                "archivedAt": now,
                "archiveReason": "superseded_by_scstrade",
                "supersededBy": {
                    "symbol": dividend["symbol"],
                    "exDate": dividend["exDate"],
                    "amountPerShare": dividend["amountPerShare"],
                },
            }
            db[archive_collection_name].replace_one({"_id": candidate["_id"]}, archived_doc, upsert=True)
            div_coll.delete_one({"_id": candidate["_id"]})
            handled.add(candidate["_id"])
            archived += 1
            logger.info(
                f"{dividend['symbol']}: archived PSX announcement {candidate['_id']} "
                f"(ex {candidate['exDate']:%Y-%m-%d}, Rs {candidate.get('amountPerShare')}) "
                f"in favour of scstrade (ex {dividend['exDate']:%Y-%m-%d}, Rs {dividend['amountPerShare']})"
            )

    return archived, blocked_keys


def correct_existing_dividends(db, dividends, dividend_collection_name, now=None):
    """
    Bring recent scstrade dividends already in the database in line with the
    amount computed from the stock's face value on the ex-date. Insert is
    otherwise the only write, so a corrected face value never reached them.
    Older records are not looked at, so a dividend that was right at the time
    of a later split stays as it is.

    Only records that are still ANNOUNCED and have no entitlements are
    changed. Others are reported, since their entitlements need recomputing
    on the backend.

    Returns the number of records updated.
    """
    now = now or datetime.now()
    if not dividends:
        return 0

    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff = today_start - timedelta(days=RECENT_LOOKBACK_DAYS)
    div_coll = db[dividend_collection_name]

    existing = {
        (doc["symbol"], doc["exDate"]): doc
        for doc in div_coll.find({"metadata.source": SCSTRADE_SOURCE, "exDate": {"$gte": cutoff}})
    }

    updated = 0
    for dividend in dividends:
        doc = existing.get((dividend["symbol"], dividend["exDate"]))
        if doc is None:
            continue

        # The amount is what credits holders. Only a different amount is a
        # correction: records saved before face values were tracked have no
        # faceValue field at all, and their amounts are right.
        if abs(doc.get("amountPerShare", 0) - dividend["amountPerShare"]) < 1e-9:
            continue

        if doc.get("status") != "ANNOUNCED" or _has_entitlements(db, doc["_id"]):
            logger.warning(
                f"{dividend['symbol']}: scstrade dividend {doc['_id']} (ex {doc['exDate']:%Y-%m-%d}) "
                f"is Rs {doc.get('amountPerShare')} at face value {doc.get('faceValue')}, but the stock "
                f"gives Rs {dividend['amountPerShare']} at {dividend['faceValue']}. It has entitlements "
                f"or is no longer ANNOUNCED, so it is left as is; repair on the backend."
            )
            continue

        div_coll.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "amountPerShare": dividend["amountPerShare"],
                "faceValue": dividend["faceValue"],
                "metadata.dividendPercent": dividend["metadata"]["dividendPercent"],
                "updatedAt": now,
            }},
        )
        updated += 1
        logger.info(
            f"{dividend['symbol']}: corrected scstrade dividend {doc['_id']} "
            f"from Rs {doc.get('amountPerShare')} to Rs {dividend['amountPerShare']} "
            f"(face value {dividend['faceValue']})"
        )

    return updated


def save_announcements_to_mongodb(df, connection_string, db_name,
                                  dividend_collection_name,
                                  bonus_collection_name,
                                  archive_collection_name=DEFAULT_ARCHIVE_COLLECTION):
    try:
        db = connect_to_mongodb(connection_string, db_name)
        create_indexes(db, dividend_collection_name, bonus_collection_name)

        face_values = load_face_values(db)
        splits = load_splits(db, bonus_collection_name)
        dividends, bonuses = process_announcements(df, face_values, splits)

        div_deleted = 0
        bonus_deleted = 0
        div_inserted = 0
        bonus_inserted = 0
        div_archived = 0
        div_corrected = 0
        blocked_keys = set()

        # Cleanup orphaned future announcements
        if not df.empty:
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

            # Cleanup dividends
            incoming_div_keys = {(d['symbol'], d['exDate']) for d in dividends}
            future_divs = db[dividend_collection_name].find({
                "exDate": {"$gte": today_start},
                "metadata.source": SCSTRADE_SOURCE
            })

            div_delete_ops = []
            for doc in future_divs:
                key = (doc.get('symbol'), doc.get('exDate'))
                if key in incoming_div_keys:
                    continue
                if _has_entitlements(db, doc['_id']):
                    logger.warning(
                        f"{doc.get('symbol')}: scstrade dividend {doc['_id']} is no longer listed "
                        f"but has entitlements, so it is kept."
                    )
                    continue
                div_delete_ops.append(pymongo.DeleteOne({'_id': doc['_id']}))

            if div_delete_ops:
                try:
                    res = db[dividend_collection_name].bulk_write(div_delete_ops, ordered=False)
                    div_deleted = res.deleted_count
                    logger.info(f"Deleted {div_deleted} orphaned future dividends")
                except Exception as e:
                    logger.warning(f"Failed to delete orphaned dividends: {e}")

            # Cleanup bonuses
            incoming_bonus_keys = {(b['symbol'], b['exDate']) for b in bonuses}
            future_bonuses = db[bonus_collection_name].find({
                "exDate": {"$gte": today_start},
                "metadata.source": SCSTRADE_SOURCE
            })

            bonus_delete_ops = []
            for doc in future_bonuses:
                key = (doc.get('symbol'), doc.get('exDate'))
                if key not in incoming_bonus_keys:
                    bonus_delete_ops.append(pymongo.DeleteOne({'_id': doc['_id']}))

            if bonus_delete_ops:
                try:
                    res = db[bonus_collection_name].bulk_write(bonus_delete_ops, ordered=False)
                    bonus_deleted = res.deleted_count
                    logger.info(f"Deleted {bonus_deleted} orphaned future bonuses")
                except Exception as e:
                    logger.warning(f"Failed to delete orphaned bonuses: {e}")

        # scstrade is authoritative: archive PSX duplicates, fix amounts after a
        # face value correction, then insert what is new.
        div_archived, blocked_keys = archive_superseded_announcements(
            db, dividends, dividend_collection_name, archive_collection_name
        )
        div_corrected = correct_existing_dividends(db, dividends, dividend_collection_name)
        dividends = [d for d in dividends if (d['symbol'], d['exDate']) not in blocked_keys]

        # Bulk write dividends
        if dividends:
            div_ops = []
            for doc in dividends:
                div_ops.append(pymongo.InsertOne(doc))

            if div_ops:
                try:
                    res = db[dividend_collection_name].bulk_write(div_ops, ordered=False)
                    div_inserted = res.inserted_count
                except pymongo.errors.BulkWriteError as bwe:
                    # Ignore duplicate key errors, count successful inserts
                    div_inserted = bwe.details['nInserted']

        # Bulk write bonuses
        if bonuses:
            bonus_ops = []
            for doc in bonuses:
                bonus_ops.append(pymongo.InsertOne(doc))

            if bonus_ops:
                try:
                    res = db[bonus_collection_name].bulk_write(bonus_ops, ordered=False)
                    bonus_inserted = res.inserted_count
                except pymongo.errors.BulkWriteError as bwe:
                    bonus_inserted = bwe.details['nInserted']

        msg = (f"Processed successfully. "
               f"Dividends: {div_inserted} inserted, {div_corrected} corrected, "
               f"{div_archived} PSX duplicates archived, {div_deleted} deleted; "
               f"Bonuses: {bonus_inserted} inserted, {bonus_deleted} deleted (duplicates skipped).")
        logger.info(msg)
        return True, msg

    except Exception as e:
        error_msg = f"Error saving data to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def get_collection_stats(connection_string, db_name,
                         dividend_collection_name, bonus_collection_name):
    try:
        db = connect_to_mongodb(connection_string, db_name)
        div_coll = db[dividend_collection_name]
        bonus_coll = db[bonus_collection_name]

        div_count = div_coll.count_documents({})
        bonus_count = bonus_coll.count_documents({})

        stats = {
            "dividends_count": div_count,
            "bonuses_count": bonus_count
        }

        # Get earliest and latest dividend dates
        div_earliest = div_coll.find_one(sort=[("exDate", pymongo.ASCENDING)])
        div_latest = div_coll.find_one(sort=[("exDate", pymongo.DESCENDING)])

        if div_earliest:
            stats["dividend_earliest_exdate"] = div_earliest.get('exDate')
        if div_latest:
            stats["dividend_latest_exdate"] = div_latest.get('exDate')

        return stats

    except Exception as e:
        logger.error(f"Error getting collection stats: {str(e)}")
        return {}
