"""
MongoDB storage functionality for PSX dividend announcements.
This module provides functions to store dividend data in MongoDB with deduplication.
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


def connect_to_mongodb(connection_string="mongodb://localhost:27017/", db_name="finhisaab"):
    """
    Connect to MongoDB and return database object.

    Args:
        connection_string (str): MongoDB connection string
        db_name (str): Name of the database

    Returns:
        pymongo.database.Database: MongoDB database object
    """
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        logger.info(f"Successfully connected to MongoDB database: {db_name}")
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise


def create_indexes(collection):
    """
    Create indexes for the dividend_announcements collection.

    Creates:
    1. Compound unique index on (symbol, x_date, dividend, bonus, right)
       to prevent duplicate announcements
    2. Descending index on x_date for date range queries
    3. Descending index on scraped_at for monitoring

    Args:
        collection: MongoDB collection object
    """
    try:
        # Compound unique index for deduplication
        collection.create_index([
            ("symbol", pymongo.ASCENDING),
            ("x_date", pymongo.ASCENDING),
            ("dividend", pymongo.ASCENDING),
            ("bonus", pymongo.ASCENDING),
            ("right", pymongo.ASCENDING)
        ], unique=True, name="unique_announcement")

        # Index for date range queries
        collection.create_index([("x_date", pymongo.DESCENDING)], name="x_date_desc")

        # Index for monitoring recent scrapes
        collection.create_index([("scraped_at", pymongo.DESCENDING)], name="scraped_at_desc")

        # Index for symbol lookup
        collection.create_index([("symbol", pymongo.ASCENDING)], name="symbol_asc")

        logger.info("Successfully created indexes for dividend_announcements collection")

    except Exception as e:
        logger.warning(f"Index creation warning (may already exist): {str(e)}")


def announcements_to_documents(announcements_df):
    """
    Convert a pandas DataFrame to a list of MongoDB documents.

    Args:
        announcements_df (pandas.DataFrame): DataFrame containing dividend announcements

    Returns:
        list: List of dictionaries in MongoDB document format
    """
    if announcements_df.empty:
        logger.warning("Empty DataFrame provided")
        return []

    documents = []

    for _, row in announcements_df.iterrows():
        # Convert pandas Timestamp to datetime if needed
        x_date = row['x_date']
        if hasattr(x_date, 'to_pydatetime'):
            x_date = x_date.to_pydatetime()

        scraped_at = row.get('scraped_at', datetime.now())
        if hasattr(scraped_at, 'to_pydatetime'):
            scraped_at = scraped_at.to_pydatetime()

        document = {
            "symbol": row['symbol'],
            "name": row['name'],
            "dividend": float(row['dividend']),
            "bonus": str(row['bonus']),
            "right": str(row['right']),
            "x_date": x_date,
            "sector": row.get('sector', ''),
            "scraped_at": scraped_at,
            "announcement_type": row['announcement_type']
        }
        documents.append(document)

    logger.info(f"Converted {len(documents)} announcements to MongoDB documents")
    return documents


def save_announcements_to_mongodb(df, connection_string="mongodb://localhost:27017/",
                                   db_name="finhisaab",
                                   collection_name="dividend_announcements"):
    """
    Save dividend announcements to MongoDB with deduplication.

    Uses bulk upsert operations to avoid duplicates. The compound unique index
    on (symbol, x_date, dividend, bonus, right) ensures no duplicate announcements.

    Args:
        df (pandas.DataFrame): DataFrame containing dividend announcements
        connection_string (str): MongoDB connection string
        db_name (str): Name of the database
        collection_name (str): Name of the collection

    Returns:
        tuple: (success, message) where success is a boolean and message is a string
    """
    try:
        # Connect to MongoDB
        db = connect_to_mongodb(connection_string, db_name)
        collection = db[collection_name]

        # Create indexes
        create_indexes(collection)

        # Convert DataFrame to documents
        documents = announcements_to_documents(df)

        if not documents:
            return False, "No documents to insert"

        # Use bulk operations for better performance
        operations = []
        for doc in documents:
            # Upsert operation: insert if not exists, update if exists
            operations.append(
                pymongo.UpdateOne(
                    {
                        "symbol": doc["symbol"],
                        "x_date": doc["x_date"],
                        "dividend": doc["dividend"],
                        "bonus": doc["bonus"],
                        "right": doc["right"]
                    },
                    {"$set": doc},
                    upsert=True
                )
            )

        # Execute bulk operations
        result = collection.bulk_write(operations)

        logger.info(f"MongoDB operation completed: {result.upserted_count} inserted, "
                   f"{result.modified_count} updated")

        return True, (f"Successfully processed {len(documents)} announcements: "
                     f"{result.upserted_count} new, {result.modified_count} updated")

    except Exception as e:
        error_msg = f"Error saving data to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def get_latest_announcements(connection_string="mongodb://localhost:27017/",
                             db_name="finhisaab",
                             collection_name="dividend_announcements",
                             days=30):
    """
    Retrieve recent dividend announcements for verification.

    Args:
        connection_string (str): MongoDB connection string
        db_name (str): Name of the database
        collection_name (str): Name of the collection
        days (int): Number of days back to query

    Returns:
        pandas.DataFrame: DataFrame with recent announcements
    """
    try:
        db = connect_to_mongodb(connection_string, db_name)
        collection = db[collection_name]

        # Query announcements with x_date in the last N days
        cutoff_date = datetime.now() - timedelta(days=days)

        cursor = collection.find(
            {"x_date": {"$gte": cutoff_date}},
            sort=[("x_date", pymongo.DESCENDING)]
        )

        # Convert to DataFrame
        announcements = list(cursor)
        if not announcements:
            logger.info(f"No announcements found in the last {days} days")
            return pd.DataFrame()

        df = pd.DataFrame(announcements)
        logger.info(f"Retrieved {len(df)} announcements from the last {days} days")

        return df

    except Exception as e:
        logger.error(f"Error retrieving announcements: {str(e)}")
        return pd.DataFrame()


def get_announcements_by_symbol(symbol, connection_string="mongodb://localhost:27017/",
                                db_name="finhisaab",
                                collection_name="dividend_announcements"):
    """
    Get all announcements for a specific symbol.

    Args:
        symbol (str): Stock symbol
        connection_string (str): MongoDB connection string
        db_name (str): Name of the database
        collection_name (str): Name of the collection

    Returns:
        pandas.DataFrame: DataFrame with announcements for the symbol
    """
    try:
        db = connect_to_mongodb(connection_string, db_name)
        collection = db[collection_name]

        cursor = collection.find(
            {"symbol": symbol},
            sort=[("x_date", pymongo.DESCENDING)]
        )

        announcements = list(cursor)
        if not announcements:
            logger.info(f"No announcements found for symbol: {symbol}")
            return pd.DataFrame()

        df = pd.DataFrame(announcements)
        logger.info(f"Retrieved {len(df)} announcements for {symbol}")

        return df

    except Exception as e:
        logger.error(f"Error retrieving announcements for {symbol}: {str(e)}")
        return pd.DataFrame()


def get_collection_stats(connection_string="mongodb://localhost:27017/",
                        db_name="finhisaab",
                        collection_name="dividend_announcements"):
    """
    Get statistics about the dividend_announcements collection.

    Returns:
        dict: Statistics including count, date range, announcement types
    """
    try:
        db = connect_to_mongodb(connection_string, db_name)
        collection = db[collection_name]

        total_count = collection.count_documents({})

        # Get date range
        earliest = collection.find_one(sort=[("x_date", pymongo.ASCENDING)])
        latest = collection.find_one(sort=[("x_date", pymongo.DESCENDING)])

        # Count by announcement type
        dividend_count = collection.count_documents(
            {"announcement_type": "dividend"}
        )
        bonus_count = collection.count_documents(
            {"announcement_type": "bonus"}
        )
        rights_count = collection.count_documents(
            {"announcement_type": "right"}
        )

        stats = {
            "total_announcements": total_count,
            "earliest_x_date": earliest['x_date'] if earliest else None,
            "latest_x_date": latest['x_date'] if latest else None,
            "dividend_announcements": dividend_count,
            "bonus_announcements": bonus_count,
            "rights_announcements": rights_count
        }

        logger.info(f"Collection stats: {stats}")
        return stats

    except Exception as e:
        logger.error(f"Error getting collection stats: {str(e)}")
        return {}


def main():
    """
    Test MongoDB storage operations.
    """
    print("=" * 80)
    print("DIVIDEND STORE - STANDALONE TEST")
    print("=" * 80)

    # Test with sample data
    sample_data = pd.DataFrame([
        {
            "symbol": "TEST",
            "name": "Test Company",
            "dividend": 15.0,
            "bonus": "",
            "right": "",
            "x_date": datetime.now(),
            "sector": "TEST SECTOR",
            "announcement_type": ["dividend"],
            "scraped_at": datetime.now()
        }
    ])

    print("\nTesting MongoDB connection and storage...")
    success, message = save_announcements_to_mongodb(
        sample_data,
        connection_string="mongodb://192.168.0.131:27017/",
        db_name="finhisaab",
        collection_name="dividend_announcements"
    )

    print(f"\nResult: {'Success' if success else 'Failed'}")
    print(f"Message: {message}")

    if success:
        print("\n✓ MongoDB storage test successful")

        # Get stats
        print("\nCollection statistics:")
        stats = get_collection_stats(
            connection_string="mongodb://192.168.0.131:27017/",
            db_name="finhisaab",
            collection_name="dividend_announcements"
        )
        for key, value in stats.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
