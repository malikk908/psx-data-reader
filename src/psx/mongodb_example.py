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

def record_processed_interval(symbol, interval_start, interval_end, connection_string, db_name, no_data_found=False):
    """
    Record a processed interval in MongoDB.
    
    Args:
        symbol (str): Stock symbol
        interval_start (datetime.date): Start date of the interval
        interval_end (datetime.date): End date of the interval
        connection_string (str): MongoDB connection string
        db_name (str): MongoDB database name
        no_data_found (bool): Whether this interval was processed but contained no data
        
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
            'processed_at': datetime.datetime.now(),
            'no_data_found': bool(no_data_found)
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

def get_stock_symbols(connection_string, db_name, batch_number=1, batch_size=10):
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
        stocks_collection = db['stocks']
        
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
    # Define the dynamic date range for daily cron run
    # Start date: 3 days before current date
    # End date: current date
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=3)

    # start_date = datetime.date(2015, 1, 1) #January 1st, 2015
    # end_date = datetime.date(2019, 12, 31) #December end, 2019

    # start_date = datetime.date(2010, 1, 1) #January 1st, 2010
    # end_date = datetime.date(2014, 12, 31) #December end, 2014

    start_date = datetime.date(2005, 1, 1) #January 1st, 2005
    end_date = datetime.date(2009, 12, 31) #December end, 2009

    # start_date = datetime.date(2000, 1, 1) #January 1st, 2000
    # end_date = datetime.date(2004, 12, 31) #December end, 2004

    # start_date = datetime.date(1995, 1, 1) #January 1st, 1995
    # end_date = datetime.date(1999, 12, 31) #December end, 1999

    # MongoDB connection settings
    connection_string = "mongodb://192.168.0.131:27017/"
    db_name = "finhisaab"
    collection_name = "stockpricehistories"

    # --- Fetch stock symbols from MongoDB in batches and process ---

    batch_size = 10
    batch_number = 1

    while True:
        print(f"\n{'='*50}")
        print(f"Fetching batch #{batch_number} of up to {batch_size} symbols...")
        symbols_to_process = get_stock_symbols(
            connection_string,
            db_name,
            batch_number=batch_number,
            batch_size=batch_size
        )

        if not symbols_to_process:
            print("No more stock symbols found or an error occurred. Exiting batch loop.")
            break

        print(f"Found {len(symbols_to_process)} symbols to process in batch #{batch_number}.")

        # Try to fetch data for the whole batch in a single call
        batch_data = None
        try:
            print(f"Attempting batch fetch for symbols: {symbols_to_process}")
            batch_data = stocks(symbols_to_process, start=start_date, end=end_date)
        except Exception as e:
            print(f"Batch fetch failed; will fallback to per-symbol fetch. Error: {e}")
            batch_data = None

        for i, symbol in enumerate(symbols_to_process):
            print(f"\nProcessing symbol: {symbol} for range {start_date} to {end_date}")

            # Skip if already processed for this full range
            if is_interval_processed(symbol, start_date, end_date, connection_string, db_name):
                print(f"Already processed for this date range. Skipping {symbol}...")
                continue

            # Resolve the DataFrame for this symbol
            try:
                symbol_df = None
                if isinstance(batch_data, dict) and symbol in batch_data:
                    symbol_df = batch_data[symbol]
                elif isinstance(batch_data, pd.DataFrame):
                    # If the batch data is a MultiIndex DataFrame with 'Ticker' level, slice it
                    try:
                        df_candidate = batch_data
                        index_names = list(df_candidate.index.names or [])
                        if 'Ticker' in index_names:
                            symbol_df = df_candidate.xs(symbol, level='Ticker')
                    except Exception:
                        symbol_df = None
                
                # Fallback to single-symbol fetch if needed
                if symbol_df is None:
                    print("Fetching individually for symbol due to unavailable batch data slice...")
                    symbol_df = stocks(symbol, start=start_date, end=end_date)

                if symbol_df is None or symbol_df.empty:
                    print(f"No data found for {symbol} in this range.")
                    record_result = record_processed_interval(
                        symbol,
                        start_date,
                        end_date,
                        connection_string,
                        db_name,
                        no_data_found=True
                    )
                    if record_result:
                        print(f"Recorded as processed (no data) for {symbol}")
                    else:
                        print(f"Failed to record as processed (no data) for {symbol}")
                    continue
                else:
                    print(f"Retrieved {len(symbol_df)} records for {symbol}")
            except Exception as e:
                print(f"An error occurred while fetching data for {symbol}: {e}")
                continue

            # Save data to MongoDB
            print(f"Saving data to MongoDB ({db_name}.{collection_name}) for {symbol}...")
            success, message = save_to_mongodb(
                df=symbol_df,
                symbol=symbol,
                connection_string=connection_string,
                db_name=db_name,
                collection_name=collection_name
            )
            print(f"MongoDB Save Result: {'Success' if success else 'Failed'}")
            print(f"Message: {message}")

            # Record as processed for this entire range
            if success:
                record_result = record_processed_interval(
                    symbol,
                    start_date,
                    end_date,
                    connection_string,
                    db_name,
                    no_data_found=False
                )
                if record_result:
                    print(f"Recorded as processed for {symbol}")
                else:
                    print(f"Failed to record as processed for {symbol}")

            # Delay between symbols to avoid overload
            if i < len(symbols_to_process) - 1:
                delay = random.uniform(1, 2)
                print(f"Waiting {delay:.2f} seconds before next symbol...")
                time.sleep(delay)

        # Delay between batches
        batch_delay = random.uniform(5, 7)
        print(f"\nCompleted batch #{batch_number}. Waiting {batch_delay:.2f} seconds before next batch...")
        time.sleep(batch_delay)

        batch_number += 1

    print(f"\n{'='*50}")
    print("All batches processed for the current run.")
 

if __name__ == "__main__":
    main()
