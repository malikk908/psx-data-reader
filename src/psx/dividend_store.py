"""
MongoDB storage functionality for PSX dividend and bonus announcements.
Aligns with Mongoose schemas DividendAnnouncement and BonusAnnouncement.
"""

import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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


def process_announcements(announcements_df):
    if announcements_df.empty:
        return [], []

    dividends = []
    bonuses = []
    
    now = datetime.now()

    for _, row in announcements_df.iterrows():
        # Get actual python datetime
        x_date = row['x_date']
        if hasattr(x_date, 'to_pydatetime'):
            x_date = x_date.to_pydatetime()

        symbol = row.get('symbol', '').strip()
        if not symbol:
            continue

        base_metadata = {
            "source": "scstrade",
            "notes": f"Sector: {row.get('sector', '')}"
        }

        # Process Dividend
        if "dividend" in row.get('announcement_type', []):
            try:
                amount = float(row.get('dividend', 0))
                if amount > 0:
                    dividends.append({
                        "symbol": symbol,
                        "amountPerShare": amount,
                        "exDate": x_date,
                        "payDate": x_date, # PSX only gives xDate, use it for required payDate
                        "status": "ANNOUNCED",
                        "metadata": base_metadata,
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

    logger.info(f"Parsed {len(dividends)} dividends and {len(bonuses)} bonuses")
    return dividends, bonuses


def save_announcements_to_mongodb(df, connection_string, db_name, 
                                  dividend_collection_name, 
                                  bonus_collection_name):
    try:
        db = connect_to_mongodb(connection_string, db_name)
        create_indexes(db, dividend_collection_name, bonus_collection_name)
        
        dividends, bonuses = process_announcements(df)
        
        div_inserted = 0
        bonus_inserted = 0

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

        msg = (f"Processed successful. "
               f"Dividends: {div_inserted} inserted; "
               f"Bonuses: {bonus_inserted} inserted (duplicates skipped).")
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
