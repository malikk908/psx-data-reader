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
    Save stock data to MongoDB using insert-only strategy.
    Duplicates (same symbol + date) are silently ignored due to unique index.

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

        # Create a unique index on symbol and date to prevent duplicates
        collection.create_index(
            [("symbol", pymongo.ASCENDING), ("date", pymongo.ASCENDING)],
            unique=True
        )

        # Add timestamps to all documents
        current_time = datetime.now()
        for doc in documents:
            doc["createdAt"] = current_time
            doc["updatedAt"] = current_time

        # Use bulk insert with ordered=False to continue on duplicate key errors
        operations = [pymongo.InsertOne(doc) for doc in documents]

        try:
            result = collection.bulk_write(operations, ordered=False)
            inserted_count = result.inserted_count
        except pymongo.errors.BulkWriteError as bwe:
            # Extract successful inserts from the error
            inserted_count = bwe.details.get('nInserted', 0)
            # Silently ignore duplicate key errors (error code 11000)
            write_errors = bwe.details.get('writeErrors', [])
            duplicate_errors = [e for e in write_errors if e.get('code') == 11000]
            other_errors = [e for e in write_errors if e.get('code') != 11000]

            if other_errors:
                # Log non-duplicate errors
                logger.warning(f"Non-duplicate errors during insert: {other_errors}")

            logger.info(f"MongoDB insert completed: {inserted_count} inserted, "
                       f"{len(duplicate_errors)} duplicates skipped")

        logger.info(f"MongoDB operation completed: {inserted_count} inserted")

        return True, f"Successfully processed {len(documents)} documents for {symbol} ({inserted_count} new, {len(documents) - inserted_count} duplicates skipped)"

    except Exception as e:
        error_msg = f"Error saving data to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
