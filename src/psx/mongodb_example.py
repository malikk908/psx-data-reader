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
        
        # Check if this interval exists in the processed_intervals collection
        query = {
            'symbol': symbol,
            'interval_start': interval_start,
            'interval_end': interval_end
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
        
        # Prepare document
        document = {
            'symbol': symbol,
            'interval_start': interval_start,
            'interval_end': interval_end,
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

def main():
    # Define the stock symbol and date range
    symbol = "MEBL"
    start_date = datetime.date(2025, 5, 15) #May 15th
    end_date = datetime.date(2025, 8, 31) #August end
    
    print(f"Fetching data for {symbol} from {start_date} to {end_date}")
    
    # Split the date range into 6-month intervals
    #2 months interval for testing
    intervals = split_date_range(start_date, end_date, months=2)
    print(f"Split into {len(intervals)} intervals:")
    for i, (interval_start, interval_end) in enumerate(intervals):
        print(f"  Interval {i+1}: {interval_start} to {interval_end}")
    
    # Initialize an empty DataFrame to store all data
    all_data = pd.DataFrame()
    
    # MongoDB connection settings
    connection_string = "mongodb://192.168.0.131:27017/"
    db_name = "finhisaab"
    collection_name = "stockpricehistories"
    
    # Fetch data for each interval
    for i, (interval_start, interval_end) in enumerate(intervals):
        print(f"\nProcessing interval {i+1}/{len(intervals)}: {interval_start} to {interval_end}")
        
        # Check if this interval has already been processed
        if is_interval_processed(symbol, interval_start, interval_end, connection_string, db_name):
            print(f"  Interval already processed. Skipping...")
            continue
            
        print(f"  Fetching data for interval: {interval_start} to {interval_end}")
        
        # Fetch stock data for this interval
        interval_data = stocks(symbol, start=interval_start, end=interval_end)
        
        print(f"  Retrieved {len(interval_data)} records")
        
        # Save this interval's data to MongoDB
        print(f"  Saving interval data to MongoDB ({db_name}.{collection_name})...")
        success, message = save_to_mongodb(
            df=interval_data,
            symbol=symbol,
            connection_string=connection_string,
            db_name=db_name,
            collection_name=collection_name
        )
        print(f"  MongoDB Save Result: {'Success' if success else 'Failed'}")
        print(f"  Message: {message}")
        
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
                print(f"  Interval recorded as processed")
            else:
                print(f"  Failed to record interval as processed")
        
        # Append to the combined DataFrame for tracking purposes
        all_data = pd.concat([all_data, interval_data])
        
        # Add random delay between API calls (except after the last interval)
        if i < len(intervals) - 1:
            delay = random.uniform(3, 6)
            print(f"  Waiting {delay:.2f} seconds before next request...")
            time.sleep(delay)
    
    # Remove any duplicate records that might occur at interval boundaries
    all_data = all_data.drop_duplicates()
    
    # Display basic information about the combined data
    print(f"\nCombined data summary:")
    print(f"Total records processed: {len(all_data)}")
    print(f"Date range: {all_data['date'].min()} to {all_data['date'].max()}")
    print(f"Columns: {all_data.columns.tolist()}")
    
    print("\nAll intervals processed and saved to MongoDB successfully.")
    
    if success:
        print("\nData has been successfully stored in MongoDB")
#         print("""
# {
#     "_id": ObjectId("..."),
#     "symbol": "MEBL", 
#     "date": ISODate("2025-08-01T00:00:00Z"),
#     "open": 360.45,
#     "high": 364.05,
#     "low": 358.02,
#     "close": 362.99,
#     "volume": 499099.0,
#     "created_at": ISODate("2025-09-04T18:39:44Z"),
#     "updated_at": ISODate("2025-09-04T18:39:44Z")
# }
#         """)
        
#         print("\nTo query this data in MongoDB, you can use:")
#         print("""
# # Using the MongoDB shell
# db.stockpricehistories.find({ "symbol": "MEBL" })

# # Get data for a specific date
# db.stockpricehistories.find({ "symbol": "MEBL", "date": ISODate("2025-08-01T00:00:00Z") })

# # Get data for a date range
# db.stockpricehistories.find({ 
#     "symbol": "MEBL", 
#     "date": { 
#         "$gte": ISODate("2025-08-01T00:00:00Z"),
#         "$lte": ISODate("2025-08-15T00:00:00Z")
#     } 
# })
#         """)

if __name__ == "__main__":
    main()
