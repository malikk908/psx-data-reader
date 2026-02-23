"""
Script for daily cron job to fetch PSX data and store it in MongoDB.
"""

from psx import stocks
from psx.data_store import save_to_mongodb
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


def get_stock_symbols(connection_string, db_name, batch_number=1, batch_size=10):
    """
    Fetch stock symbols from the 'stock' collection in MongoDB, sorted by marketCap.
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
        if 'client' in locals():
            client.close()

def main():
    # Define the dynamic date range for daily cron run
    # Start date: yesterday
    # End date: today
    end_date = datetime.date.today()
    start_date = end_date - datetime.timedelta(days=1)

    # MongoDB connection settings via environment variables
    # Provide sensible defaults for local development
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    # Early connectivity test to fail fast if DB is unreachable
    if not test_mongo_connectivity(connection_string, db_name):
        print("Exiting due to MongoDB connectivity failure.")
        return

    # --- Fetch stock symbols from MongoDB in batches and process ---

    # Batching and throttling configuration via environment variables
    batch_size = int(os.getenv("FINHISAAB_BATCH_SIZE", "10"))
    max_batches_env = os.getenv("FINHISAAB_MAX_BATCHES", "")
    max_batches = int(max_batches_env) if max_batches_env.strip().isdigit() else None

    # Optional throttling controls
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2"))
    batch_delay_min = float(os.getenv("FINHISAAB_BATCH_DELAY_MIN", "5"))
    batch_delay_max = float(os.getenv("FINHISAAB_BATCH_DELAY_MAX", "7"))

    batch_number = 1
    processed_batches = 0

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

            # Delay between symbols to avoid overload
            if i < len(symbols_to_process) - 1:
                delay = random.uniform(symbol_delay_min, symbol_delay_max)
                print(f"Waiting {delay:.2f} seconds before next symbol...")
                time.sleep(delay)

        # Delay between batches
        batch_delay = random.uniform(batch_delay_min, batch_delay_max)
        print(f"\nCompleted batch #{batch_number}. Waiting {batch_delay:.2f} seconds before next batch...")
        time.sleep(batch_delay)

        batch_number += 1
        processed_batches += 1

        # If max_batches is set (e.g., for local testing), stop after processing that many batches
        if max_batches is not None and processed_batches >= max_batches:
            print(f"Reached FINHISAAB_MAX_BATCHES={max_batches}. Stopping further batch processing for this run.")
            break

    print(f"\n{'='*50}")
    print("All batches processed for the current run.")
 

if __name__ == "__main__":
    main()
