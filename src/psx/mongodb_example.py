"""
Example script demonstrating how to fetch PSX data and store it in MongoDB.
"""

from psx import stocks
from psx.data_store import save_to_mongodb
import datetime
import time
import random
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError

def is_interval_processed(symbol, interval_start, interval_end, connection_string, db_name):
    """
    Check if a specific interval for a stock has already been processed and stored in MongoDB.
    
    Args:
        symbol (str): Stock symbol
        interval_start (datetime.date): Start date of the interval
        interval_end (datetime.date): End date of the interval
        connection_string (str): MongoDB connection string
        db_name (str): MongoDB database name
        
    Returns:
        bool: True if the interval has been processed, False otherwise
    """
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[db_name]
        processed_intervals = db['processed_intervals']
        
        # Convert date objects to datetime objects for MongoDB compatibility
        start_datetime = datetime.datetime.combine(interval_start, datetime.time.min)
        end_datetime = datetime.datetime.combine(interval_end, datetime.time.min)
        
        # Check if this interval exists in the processed_intervals collection
        query = {
            'symbol': symbol,
            'interval_start': start_datetime,
            'interval_end': end_datetime
        }
        
        result = processed_intervals.find_one(query)
        return result is not None
        
    except PyMongoError as e:
        print(f"Error checking processed intervals: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()

def record_processed_interval(symbol, interval_start, interval_end, connection_string, db_name):
    """
    Record a processed interval in MongoDB.
    
    Args:
        symbol (str): Stock symbol
        interval_start (datetime.date): Start date of the interval
        interval_end (datetime.date): End date of the interval
        connection_string (str): MongoDB connection string
        db_name (str): MongoDB database name
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[db_name]
        processed_intervals = db['processed_intervals']
        
        # Convert date objects to datetime objects for MongoDB compatibility
        start_datetime = datetime.datetime.combine(interval_start, datetime.time.min)
        end_datetime = datetime.datetime.combine(interval_end, datetime.time.min)
        
        # Prepare document
        document = {
            'symbol': symbol,
            'interval_start': start_datetime,
            'interval_end': end_datetime,
            'processed_at': datetime.datetime.now()
        }
        
        # Insert the document
        processed_intervals.insert_one(document)
        return True
        
    except PyMongoError as e:
        print(f"Error recording processed interval: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()

def split_date_range(start_date, end_date, months=6):
    """
    Split a date range into intervals of specified months.
    
    Args:
        start_date (datetime.date): Start date
        end_date (datetime.date): End date
        months (int): Number of months per interval
        
    Returns:
        list: List of (interval_start, interval_end) tuples
    """
    intervals = []
    current_start = start_date
    
    while current_start < end_date:
        # Calculate the end of this interval (current_start + months)
        # Add months by calculating year and month separately
        year = current_start.year + ((current_start.month - 1 + months) // 12)
        month = ((current_start.month - 1 + months) % 12) + 1
        day = min(current_start.day, [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 
                                     31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month-1])
        
        interval_end = datetime.date(year, month, day)
        
        # If this interval would go past the end_date, cap it
        if interval_end > end_date:
            interval_end = end_date
            
        intervals.append((current_start, interval_end))
        
        # Move to the next interval
        # Add one day to avoid overlapping dates
        current_start = interval_end + datetime.timedelta(days=1)
    
    return intervals

def get_stock_symbols(connection_string, db_name, batch_number=1, batch_size=2):
    """
    Fetch stock symbols from the 'stock' collection in MongoDB, sorted by marketCap.

    Args:
        connection_string (str): MongoDB connection string.
        db_name (str): MongoDB database name.
        batch_number (int): The batch number to fetch (e.g., 1 for the first 50, 2 for the next 50).
        batch_size (int): The number of stocks in each batch.

    Returns:
        list: A list of stock symbols.
    """
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        stocks_collection = db['stock']
        
        # Calculate the number of documents to skip
        skip_amount = (batch_number - 1) * batch_size
        
        # Fetch symbols, sorted by marketCap descending
        symbols = stocks_collection.find({}, {'symbol': 1, '_id': 0}) \
                                   .sort('marketCap', -1) \
                                   .skip(skip_amount) \
                                   .limit(batch_size)
        
        return [s['symbol'] for s in symbols]
    
    except PyMongoError as e:
        print(f"Error fetching stock symbols: {e}")
        return []
    finally:
        if 'client' in locals():
            client.close()

def main():
    # Define the stock symbol and date range
    start_date = datetime.date(2020, 1, 1) #January 1st
    end_date = datetime.date(2025, 8, 31) #August end

    # MongoDB connection settings
    connection_string = "mongodb://192.168.0.131:27017/"
    db_name = "finhisaab"
    collection_name = "stockpricehistories"

    # --- Fetch stock symbols from MongoDB ---
    # Change the batch_number to process different sets of 50 stocks
    batch_number_to_process = 1 
    symbols_to_process = get_stock_symbols(connection_string, db_name, batch_number=batch_number_to_process)

    if not symbols_to_process:
        print("No stock symbols found or an error occurred. Exiting.")
        return

    print(f"Found {len(symbols_to_process)} symbols to process in batch #{batch_number_to_process}.")

    # --- Loop over each symbol and process it ---
    for symbol in symbols_to_process:
        print(f"\n{'='*50}")
        print(f"Processing symbol: {symbol}")
        print(f"Fetching data for {symbol} from {start_date} to {end_date}")
        
        # Split the date range into 6-month intervals
        intervals = split_date_range(start_date, end_date, months=6)
        print(f"Split into {len(intervals)} intervals.")

        # Fetch data for each interval
        for i, (interval_start, interval_end) in enumerate(intervals):
            print(f"\n  Processing interval {i+1}/{len(intervals)}: {interval_start} to {interval_end}")
            
            # Check if this interval has already been processed
            if is_interval_processed(symbol, interval_start, interval_end, connection_string, db_name):
                print(f"    Interval already processed for {symbol}. Skipping...")
                continue
                
            print(f"    Fetching data for {symbol}...")
            
            # Fetch stock data for this interval
            try:
                interval_data = stocks(symbol, start=interval_start, end=interval_end)
                if interval_data.empty:
                    print(f"    No data found for {symbol} in this interval.")
                    continue
                print(f"    Retrieved {len(interval_data)} records for {symbol}")
            except Exception as e:
                print(f"    An error occurred while fetching data for {symbol}: {e}")
                continue # Move to the next interval

            # Save this interval's data to MongoDB
            print(f"    Saving interval data to MongoDB ({db_name}.{collection_name})...")
            success, message = save_to_mongodb(
                df=interval_data,
                symbol=symbol,
                connection_string=connection_string,
                db_name=db_name,
                collection_name=collection_name
            )
            print(f"    MongoDB Save Result: {'Success' if success else 'Failed'}")
            print(f"    Message: {message}")
            
            # If save was successful, record this interval as processed
            if success:
                record_result = record_processed_interval(
                    symbol, 
                    interval_start, 
                    interval_end, 
                    connection_string, 
                    db_name
                )
                if record_result:
                    print(f"    Interval recorded as processed for {symbol}")
                else:
                    print(f"    Failed to record interval as processed for {symbol}")
            
            # Add random delay between API calls
            if i < len(intervals) - 1:
                delay = random.uniform(6, 10)
                print(f"    Waiting {delay:.2f} seconds before next request...")
                time.sleep(delay)

    print(f"\n{'='*50}")
    print("All specified stock symbols and their intervals processed.")
 

if __name__ == "__main__":
    main()
