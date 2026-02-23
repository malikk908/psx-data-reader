"""
Script to identify missing price data intervals for PSX stocks in MongoDB.
"""

import os
import datetime
import json
import random
import time
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError


# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def get_stock_symbols(connection_string, db_name, batch_number=1, batch_size=10):
    """
    Fetch stock symbols from the 'stocks' collection in MongoDB.
    """
    client = None
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
        if 'client' in locals() and client:
            client.close()

def find_missing_dates(symbol, start_date, end_date, connection_string, db_name, collection_name, exclusions=None):
    """
    Finds missing business dates in MongoDB for a specific stock within a date range.
    Returns a list of missing dates (as datetime.date objects).
    """
    client = None
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]

        # Convert date to datetime.datetime for MongoDB query
        start_dt = datetime.datetime.combine(start_date, datetime.time.min)
        end_dt = datetime.datetime.combine(end_date, datetime.time.max)

        # 1. Fetch dates currently in the DB
        # Retrieve only the 'date' field
        docs = collection.find(
            {
                'symbol': symbol,
                'date': {'$gte': start_dt, '$lte': end_dt}
            },
            {'date': 1, '_id': 0}
        )
        
        # Set of dates existing in DB
        db_dates = {doc['date'].date() for doc in docs}

        # 2. Generate expected business dates (Mon-Fri)
        expected_dates = pd.bdate_range(start=start_date, end=end_date)
        expected_dates_set = {pd.Timestamp(dt).date() for dt in expected_dates}

        if exclusions:
            for ex in exclusions:
                ex_start = datetime.datetime.strptime(ex['start'], "%Y-%m-%d").date()
                ex_end = datetime.datetime.strptime(ex['end'], "%Y-%m-%d").date()
                ex_dates = pd.bdate_range(start=ex_start, end=ex_end)
                ex_dates_set = {pd.Timestamp(dt).date() for dt in ex_dates}
                expected_dates_set -= ex_dates_set

        # 3. Find missing dates
        missing_dates = sorted(list(expected_dates_set - db_dates))
        
        return missing_dates
        
    except PyMongoError as e:
        print(f"Error querying database for {symbol}: {e}")
        return []
    finally:
        if 'client' in locals() and client:
            client.close()

def group_missing_dates(missing_dates, max_ignored_gap_size=1):
    """
    Groups a sorted list of consecutive dates into ranges (Start Date -> End Date).
    Returns a list of dictionaries: [{'start': 'YYYY-MM-DD', 'end': 'YYYY-MM-DD'}, ...]
    Ranges with a business day duration <= max_ignored_gap_size are ignored.
    """
    if not missing_dates:
        return []

    ranges = []
    current_start = missing_dates[0]
    current_end = missing_dates[0]

    for i in range(1, len(missing_dates)):
        next_dt = missing_dates[i]
        
        # Check if the next datetime is consecutive business day.
        # It's better to just check if it's within 3 days (handling weekends)
        diff = (next_dt - current_end).days
        if diff <= 3: # Consecutive business days could be Friday->Monday (3 days diff)
            current_end = next_dt
        else:
            # Check length of gap in business days
            if len(pd.bdate_range(current_start, current_end)) > max_ignored_gap_size:
                ranges.append({
                    "start": current_start.strftime('%Y-%m-%d'),
                    "end": current_end.strftime('%Y-%m-%d')
                })
            current_start = next_dt
            current_end = next_dt

    # Append the last range
    if len(pd.bdate_range(current_start, current_end)) > max_ignored_gap_size:
        ranges.append({
            "start": current_start.strftime('%Y-%m-%d'),
            "end": current_end.strftime('%Y-%m-%d')
        })

    return ranges

def main():
    # --- Configuration ---
    # Configure the global date range you want to check for missing data
    start_date = datetime.date(2020, 1, 1)
    end_date = datetime.date(2025, 9, 26)

    # Exclusions
    exclusions = [
        {
            "start": "2025-02-05",
            "end": "2025-02-05"
        },
        {
            "start": "2025-03-28",
            "end": "2025-04-02"
        },
        {
            "start": "2025-05-01",
            "end": "2025-05-01"
        },
        {
            "start": "2025-05-28",
            "end": "2025-05-28"
        },
        {
            "start": "2025-06-06",
            "end": "2025-06-09"
        },
        {
            "start": "2025-08-14",
            "end": "2025-08-14"
        },
         {
            "start": "2020-05-22",
            "end": "2020-05-27"
        },
        {
            "start": "2021-05-07",
            "end": "2021-05-14"
        },
        {
            "start": "2021-07-20",
            "end": "2021-07-22"
        },
        {
            "start": "2022-04-29",
            "end": "2022-05-05"
        },
        {
            "start": "2022-07-08",
            "end": "2022-07-12"
        },
        {
            "start": "2023-04-21",
            "end": "2023-04-25"
        },
        {
            "start": "2023-06-28",
            "end": "2023-06-30"
        },
        {
            "start": "2024-04-10",
            "end": "2024-04-12"
        },
        {
            "start": "2024-06-17",
            "end": "2024-06-19"
        },
    ]

    # MongoDB connection settings via environment variables
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://127.0.0.1:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    # Batching configuration
    batch_size = int(os.getenv("FINHISAAB_BATCH_SIZE", "10"))
    max_batches_env = os.getenv("FINHISAAB_MAX_BATCHES", "None")  
    max_batches = int(max_batches_env) if max_batches_env and max_batches_env.strip().isdigit() else None
    
    # Gap filtering configuration
    max_ignored_gap_size = int(os.getenv("FINHISAAB_MAX_IGNORED_GAP_SIZE", "1"))

    # Output file
    output_filename = "missing_data_report.json"

    print(f"Checking for missing PSX data between {start_date} and {end_date}")
    print(f"DB: {db_name}.{collection_name}")
    print("-" * 50)

    batch_number = 1
    processed_batches = 0
    
    all_missing_data = {} # Format: { "symbol": [{"start": "...", "end": "..."}] }
    total_symbols_with_gaps = 0

    while True:
        symbols_to_process = get_stock_symbols(
            connection_string,
            db_name,
            batch_number=batch_number,
            batch_size=batch_size
        )

        if not symbols_to_process:
            break

        print(f"\nProcessing Batch #{batch_number} ({len(symbols_to_process)} symbols)...")

        for symbol in symbols_to_process:
            missing_dates = find_missing_dates(
                symbol, start_date, end_date, 
                connection_string, db_name, collection_name,
                exclusions
            )

            if missing_dates:
                ranges = group_missing_dates(missing_dates, max_ignored_gap_size)
                
                if ranges:
                    all_missing_data[symbol] = ranges
                    total_symbols_with_gaps += 1
                    print(f"[{symbol}] Missing {len(missing_dates)} business days -> {len(ranges)} date ranges (> {max_ignored_gap_size} days)")
                    for r in ranges:
                        print(f"  - {r['start']} to {r['end']}")
                else:
                    print(f"[{symbol}] Data completely up to date (or only small gaps <= {max_ignored_gap_size} days).")
            else:
                print(f"[{symbol}] Data completely up to date.")

        batch_number += 1
        processed_batches += 1

        if max_batches is not None and processed_batches >= max_batches:
            print(f"Reached FINHISAAB_MAX_BATCHES={max_batches}. Stopping check.")
            break

    print("\n" + "=" * 50)
    print("FINISHED MISSING DATA CHECK")
    print(f"Analyzed {processed_batches * batch_size} symbols.")
    print(f"Found missing data for {total_symbols_with_gaps} symbols.")
    print("=" * 50)

    # Save absolute report to JSON file
    if all_missing_data:
        try:
            with open(output_filename, 'w') as f:
                json.dump(all_missing_data, f, indent=4)
            print(f"Detailed JSON report saved to {os.path.abspath(output_filename)}")
        except Exception as e:
            print(f"Failed to save JSON report: {e}")

if __name__ == "__main__":
    main()
