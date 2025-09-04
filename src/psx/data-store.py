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

def connect_to_mongodb(connection_string="mongodb://localhost:27017/", db_name="psx_stocks"):
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
    
    # Current timestamp for created_at and updated_at fields
    current_time = datetime.now()
    
    documents = []
    for _, row in df_reset.iterrows():
        # Convert pandas Timestamp to datetime if needed
        date = row['Date']
        if hasattr(date, 'to_pydatetime'):
            date = date.to_pydatetime()
        
        document = {
            "symbol": symbol,
            "date": date,
            "open": float(row['Open']),
            "high": float(row['High']),
            "low": float(row['Low']),
            "close": float(row['Close']),
            "volume": float(row['Volume']),
            "created_at": current_time,
            "updated_at": current_time
        }
        documents.append(document)
    
    logger.info(f"Converted {len(documents)} rows of data for symbol {symbol}")
    return documents

def save_to_mongodb(df, symbol, connection_string="mongodb://localhost:27017/", 
                   db_name="psx_stocks", collection_name="stock_data"):
    """
    Save stock data to MongoDB
    
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
        
        # Create a unique index on symbol and date to avoid duplicates
        collection.create_index([("symbol", pymongo.ASCENDING), ("date", pymongo.ASCENDING)], unique=True)
        
        # Use bulk operations for better performance
        operations = []
        for doc in documents:
            # Upsert operation: insert if not exists, update if exists
            operations.append(
                pymongo.UpdateOne(
                    {"symbol": doc["symbol"], "date": doc["date"]},
                    {"$set": doc},
                    upsert=True
                )
            )
        
        # Execute bulk operations
        result = collection.bulk_write(operations)
        
        logger.info(f"MongoDB operation completed: {result.upserted_count} inserted, "
                   f"{result.modified_count} updated")
        
        return True, f"Successfully processed {len(documents)} documents for {symbol}"
    
    except Exception as e:
        error_msg = f"Error saving data to MongoDB: {str(e)}"
        logger.error(error_msg)
        return False, error_msg
