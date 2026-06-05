"""
Script for daily cron job to fetch PSX data and store it in MongoDB.

Supports parallel execution via --start / --end flags (1-based rank in
market-cap-sorted stock list).  Example — split 550 stocks across 10 workers:
    python mongodb_cron.py --start 1   --end 55
    python mongodb_cron.py --start 56  --end 110
    ...
Omit both flags to process all stocks (default / backward-compatible).
"""

from psx import stocks
from psx.data_store import save_to_mongodb
import argparse
import datetime
import time
import random
import pandas as pd
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def test_mongo_connectivity(connection_string: str, db_name: str) -> bool:
    """
    Perform a quick connectivity test to MongoDB.

    Attempts to connect and run a simple ping command. Returns True if
    successful, False otherwise.
    """
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        # Run a simple admin ping to verify connectivity
        client.admin.command('ping')
        # Optionally touch the target DB to ensure we can access it
        _ = client[db_name].name
        print(f"MongoDB connectivity OK for {connection_string} (db: {db_name})")
        return True
    except Exception as e:
        print(f"MongoDB connectivity check FAILED: {e}")
        return False
    finally:
        try:
            if 'client' in locals():
                client.close()
        except Exception:
            pass


def get_stock_symbols_range(connection_string, db_name, start_rank=1, end_rank=None):
    """
    Fetch stock symbols from MongoDB sorted by marketCap descending.

    start_rank / end_rank are 1-based and inclusive.  end_rank=None means
    fetch from start_rank to the end of the collection.
    """
    client = None
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        stocks_collection = db['stocks']

        skip = start_rank - 1
        limit = (end_rank - start_rank + 1) if end_rank is not None else 0

        cursor = stocks_collection.find({}, {'symbol': 1, '_id': 0}) \
                                  .sort('marketCap', -1) \
                                  .skip(skip)
        if limit > 0:
            cursor = cursor.limit(limit)

        return [s['symbol'] for s in cursor]

    except PyMongoError as e:
        print(f"Error fetching stock symbols: {e}")
        return []
    finally:
        if client is not None:
            client.close()

def main():
    parser = argparse.ArgumentParser(description="PSX daily data fetch cron job.")
    parser.add_argument(
        "--start", type=int, default=1,
        help="1-based start rank in market-cap-sorted stock list (default: 1)"
    )
    parser.add_argument(
        "--end", type=int, default=None,
        help="1-based end rank, inclusive (default: all stocks from --start)"
    )
    args = parser.parse_args()

    start_rank = args.start
    end_rank = args.end

    range_label = f"ranks {start_rank}–{end_rank}" if end_rank else f"ranks {start_rank}–end"
    print(f"Processing stock {range_label}")
    failed_symbols = []

    # Check if today is Sunday (6)
    today = datetime.date.today()
    weekday = today.weekday()

    if weekday == 6:
        print(f"Today is Sunday ({today}). Skipping PSX data fetch as it's a holiday.")
        return

    # Define the dynamic date range based on the day of the week
    if weekday == 0:  # Monday
        start_date = today
        end_date = today
        print(f"Monday run detected. Fetching data only for today: {today}")
    elif weekday == 5:  # Saturday
        friday = today - datetime.timedelta(days=1)
        start_date = friday
        end_date = friday
        print(f"Saturday run detected. Fetching data for Friday: {friday}")
    else:
        start_date = today - datetime.timedelta(days=1)
        end_date = today
        print(f"Weekday run detected. Fetching data from {start_date} to {end_date}")

    # MongoDB connection settings via environment variables
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    if not test_mongo_connectivity(connection_string, db_name):
        print("Exiting due to MongoDB connectivity failure.")
        return

    # Throttling configuration via environment variables
    batch_size = int(os.getenv("FINHISAAB_BATCH_SIZE", "10"))
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "0.3"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "0.7"))
    batch_delay_min = float(os.getenv("FINHISAAB_BATCH_DELAY_MIN", "1"))
    batch_delay_max = float(os.getenv("FINHISAAB_BATCH_DELAY_MAX", "2"))

    # Fetch the assigned slice of symbols from MongoDB in one query
    all_symbols = get_stock_symbols_range(connection_string, db_name, start_rank, end_rank)

    if not all_symbols:
        print("No stock symbols found for the specified range. Exiting.")
        return

    print(f"Total symbols to process: {len(all_symbols)}")

    # Chunk into internal batches for throttling
    batches = [all_symbols[i:i + batch_size] for i in range(0, len(all_symbols), batch_size)]

    for batch_number, symbols_to_process in enumerate(batches, start=1):
        print(f"\n{'='*50}")
        print(f"Batch {batch_number}/{len(batches)}: {len(symbols_to_process)} symbols")

        batch_data = None
        try:
            print(f"Attempting batch fetch for symbols: {symbols_to_process}")
            batch_data = stocks(symbols_to_process, start=start_date, end=end_date)
        except Exception as e:
            print(f"Batch fetch failed; will fallback to per-symbol fetch. Error: {e}")

        for i, symbol in enumerate(symbols_to_process):
            print(f"\nProcessing symbol: {symbol} for range {start_date} to {end_date}")

            try:
                symbol_df = None
                if isinstance(batch_data, dict) and symbol in batch_data:
                    symbol_df = batch_data[symbol]
                elif isinstance(batch_data, pd.DataFrame):
                    try:
                        index_names = list(batch_data.index.names or [])
                        if 'Ticker' in index_names:
                            symbol_df = batch_data.xs(symbol, level='Ticker')
                    except Exception:
                        symbol_df = None

                if symbol_df is None:
                    print("Fetching individually for symbol due to unavailable batch data slice...")
                    symbol_df = stocks(symbol, start=start_date, end=end_date)

                if symbol_df is None or symbol_df.empty:
                    print(f"No data found for {symbol} in this range.")
                else:
                    print(f"Retrieved {len(symbol_df)} records for {symbol}")
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
            except Exception as e:
                print(f"An error occurred while fetching data for {symbol}: {e}")
                failed_symbols.append((symbol, str(e)))

            # Delay between symbols to avoid overload (always runs even if errors occurred)
            if i < len(symbols_to_process) - 1:
                delay = random.uniform(symbol_delay_min, symbol_delay_max)
                print(f"Waiting {delay:.2f} seconds before next symbol...")
                time.sleep(delay)

        if batch_number < len(batches):
            batch_delay = random.uniform(batch_delay_min, batch_delay_max)
            print(f"\nCompleted batch {batch_number}. Waiting {batch_delay:.2f} seconds...")
            time.sleep(batch_delay)

    print(f"\n{'='*50}")
    print(f"All {len(all_symbols)} symbols processed for {range_label}.")

    if failed_symbols:
        raise RuntimeError(f"Scraping failed for the following {len(failed_symbols)} symbols: {failed_symbols}")
 

if __name__ == "__main__":
    main()
