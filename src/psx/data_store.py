"""
MongoDB storage functionality for PSX stock data.
This module provides functions to store stock data in MongoDB.
"""

import pymongo
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb(connection_string="mongodb://localhost:27017/", db_name="finhisaab"):
    """
    Connect to MongoDB and return database object
    
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

def dataframe_to_documents(df, symbol):
    """
    Convert a pandas DataFrame to a list of MongoDB documents

    Args:
        df (pandas.DataFrame): DataFrame containing stock data
        symbol (str): Stock symbol

    Returns:
        list: List of dictionaries in MongoDB document format
    """
    if df.empty:
        logger.warning(f"Empty DataFrame provided for symbol {symbol}")
        return []

    # Reset index to make the Date column accessible
    df_reset = df.reset_index()

    documents = []
    for _, row in df_reset.iterrows():
        # Convert pandas Timestamp to datetime if needed
        date = row['Date']
        if hasattr(date, 'to_pydatetime'):
            date = date.to_pydatetime()

        document = {
            "symbol": symbol,
            "date": date,
            "open": int(round(float(row['Open']) * 100)),
            "high": int(round(float(row['High']) * 100)),
            "low": int(round(float(row['Low']) * 100)),
            "close": int(round(float(row['Close']) * 100)),
            "volume": float(row['Volume'])
        }
        documents.append(document)

    logger.info(f"Converted {len(documents)} rows of data for symbol {symbol}")
    return documents

def save_to_mongodb(df, symbol, connection_string="mongodb://localhost:27017/",
                   db_name="finhisaab", collection_name="psxstockpricedata"):
    """
    Save stock data to MongoDB using an upsert strategy.
    If a record with the same (symbol, date) already exists it is updated with
    the latest scraped values; otherwise a new document is inserted.

    Args:
        df (pandas.DataFrame): DataFrame containing stock data
        symbol (str): Stock symbol
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

        # Convert DataFrame to documents
        documents = dataframe_to_documents(df, symbol)

        if not documents:
            return False, "No documents to insert"

        # Ensure a unique index on (symbol, date).
        # Upserts (UpdateOne) work fine with unique indexes — only raw InsertOne
        # operations raise duplicate-key errors, which we no longer use.
        collection.create_index(
            [("symbol", pymongo.ASCENDING), ("date", pymongo.ASCENDING)],
            unique=True
        )

        # Build upsert operations — match on (symbol, date), overwrite all fields
        current_time = datetime.now()
        operations = []
        for doc in documents:
            filter_keys = {"symbol": doc["symbol"], "date": doc["date"]}
            update_fields = {k: v for k, v in doc.items() if k not in ("symbol", "date")}
            update_fields["updatedAt"] = current_time
            set_on_insert = {"createdAt": current_time}

            operations.append(
                pymongo.UpdateOne(
                    filter_keys,
                    {
                        "$set": update_fields,
                        "$setOnInsert": set_on_insert,
                    },
                    upsert=True,
                )
            )

        result = collection.bulk_write(operations, ordered=False)
        inserted_count = result.upserted_count
        updated_count = result.modified_count

        logger.info(
            f"MongoDB upsert completed for {symbol}: "
            f"{inserted_count} inserted, {updated_count} updated"
        )

        return True, (
            f"Successfully processed {len(documents)} documents for {symbol} "
            f"({inserted_count} new, {updated_count} updated)"
        )

    except Exception as e:
        error_msg = f"Error saving data to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
