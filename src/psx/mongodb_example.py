"""
Example script demonstrating how to fetch PSX data and store it in MongoDB.
"""

from psx import stocks
from psx.data_store import save_to_mongodb
import datetime
import time
import random
import pandas as pd
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError, DuplicateKeyError

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

def record_failed_interval(symbol, interval_start, interval_end, connection_string, db_name, reason):
    """
    Record a failed data fetch interval in MongoDB for manual review later.
    Duplicates are skipped/updated silently.
    
    Args:
        symbol (str): Stock symbol
        interval_start (datetime.date): Start date of the interval
        interval_end (datetime.date): End date of the interval
        connection_string (str): MongoDB connection string
        db_name (str): MongoDB database name
        reason (str): Reason for failure
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        db = client[db_name]
        failed_intervals = db['failed_intervals']
        
        # Convert date objects to datetime objects for MongoDB compatibility
        start_datetime = datetime.datetime.combine(interval_start, datetime.time.min)
        end_datetime = datetime.datetime.combine(interval_end, datetime.time.min)
        now = datetime.datetime.now()
        
        document = {
            'symbol': symbol,
            'interval_start': start_datetime,
            'interval_end': end_datetime,
            'reason': reason,
            'createdAt': now,
            'updatedAt': now
        }
        
        try:
            failed_intervals.insert_one(document)
        except DuplicateKeyError:
            # Silently skip if it already exists due to unique index
            pass
            
        return True
        
    except PyMongoError as e:
        print(f"Error recording failed interval: {e}")
        return False
    finally:
        if 'client' in locals():
            client.close()

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

def check_symbols_data_coverage(
    symbols,
    start_date,
    end_date,
    connection_string,
    db_name,
    collection_name,
    tolerance_days=5
):
    """
    Check which symbols have sufficient data coverage in MongoDB for a date range.
    Uses a single aggregation query for all symbols (batch optimized).

    Args:
        symbols (list): List of stock symbols to check.
        start_date (datetime.date): Start date of the range to check.
        end_date (datetime.date): End date of the range to check.
        connection_string (str): MongoDB connection string.
        db_name (str): Database name.
        collection_name (str): Collection name.
        tolerance_days (int): Number of days margin before/after range (default: 5).

    Returns:
        dict: Dictionary with coverage info per symbol:
            {
                'SYMBOL': {
                    'has_coverage': bool,
                    'record_count': int,
                    'first_date': datetime.date or None,
                    'last_date': datetime.date or None,
                    'should_skip': bool
                }
            }
    """
    if not symbols:
        return {}

    client = None
    try:
        client = MongoClient(connection_string)
        db = client[db_name]
        collection = db[collection_name]

        # Expand date range with tolerance margin
        start_dt_with_margin = datetime.datetime.combine(
            start_date - datetime.timedelta(days=tolerance_days),
            datetime.time.min
        )
        end_dt_with_margin = datetime.datetime.combine(
            end_date + datetime.timedelta(days=tolerance_days),
            datetime.time.max
        )

        # Single aggregation query for all symbols
        pipeline = [
            {
                '$match': {
                    'symbol': {'$in': symbols},
                    'date': {'$gte': start_dt_with_margin, '$lte': end_dt_with_margin}
                }
            },
            {
                '$group': {
                    '_id': '$symbol',
                    'count': {'$sum': 1},
                    'first_date': {'$min': '$date'},
                    'last_date': {'$max': '$date'}
                }
            }
        ]

        results = collection.aggregate(pipeline)

        # Calculate minimum expected records
        total_days = (end_date - start_date).days + 1
        min_expected_records = max(1, int(total_days * 0.5))

        # Build coverage info for symbols with data
        coverage_info = {}
        symbols_with_data = set()

        for result in results:
            symbol = result['_id']
            first_date = result['first_date'].date() if result['first_date'] else None
            last_date = result['last_date'].date() if result['last_date'] else None
            count = result['count']

            # Determine coverage
            has_start_coverage = first_date <= start_date if first_date else False
            has_end_coverage = last_date >= end_date if last_date else False
            has_sufficient_records = count >= min_expected_records

            has_coverage = has_start_coverage and has_end_coverage and has_sufficient_records

            coverage_info[symbol] = {
                'has_coverage': has_coverage,
                'record_count': count,
                'first_date': first_date,
                'last_date': last_date,
                'should_skip': has_coverage
            }
            symbols_with_data.add(symbol)

        # Add symbols with NO data
        for symbol in symbols:
            if symbol not in symbols_with_data:
                coverage_info[symbol] = {
                    'has_coverage': False,
                    'record_count': 0,
                    'first_date': None,
                    'last_date': None,
                    'should_skip': False
                }

        return coverage_info

    except PyMongoError as e:
        print(f"Error checking data coverage: {e}")
        # Fail open: return no coverage for all symbols (fetch everything)
        return {symbol: {
            'has_coverage': False,
            'record_count': 0,
            'first_date': None,
            'last_date': None,
            'should_skip': False
        } for symbol in symbols}
    finally:
        if client:
            client.close()

def main():
    # Define the dynamic date range for daily cron run
    start_date = datetime.date(2015, 7, 1) #July 1st, 2015
    end_date = datetime.date(2015, 12, 31) # December 31st, 2015
    
    # MongoDB connection settings via environment variables
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    # Early connectivity test to fail fast if DB is unreachable
    if not test_mongo_connectivity(connection_string, db_name):
        print("Exiting due to MongoDB connectivity failure.")
        return

    # --- Fetch stock symbols from MongoDB in batches and process ---

    # Batching and throttling configuration via environment variables
    batch_size = int(os.getenv("FINHISAAB_BATCH_SIZE", "20"))
    max_batches_env = os.getenv("FINHISAAB_MAX_BATCHES", "")
    max_batches = int(max_batches_env) if max_batches_env.strip().isdigit() else None

    # Optional throttling controls
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2"))
    batch_delay_min = float(os.getenv("FINHISAAB_BATCH_DELAY_MIN", "5"))
    batch_delay_max = float(os.getenv("FINHISAAB_BATCH_DELAY_MAX", "7"))

    batch_number = 1
    processed_batches = 0

    # Performance tracking
    total_symbols_processed = 0
    total_symbols_skipped = 0
    total_symbols_fetched = 0

    try:
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

            # Check data coverage for batch optimization
            print(f"Checking data coverage for {len(symbols_to_process)} symbols...")
            coverage_info = check_symbols_data_coverage(
                symbols=symbols_to_process,
                start_date=start_date,
                end_date=end_date,
                connection_string=connection_string,
                db_name=db_name,
                collection_name=collection_name,
                tolerance_days=5
            )

            # Filter symbols: separate those with coverage from those needing fetch
            symbols_with_coverage = [s for s in symbols_to_process
                                     if coverage_info[s]['should_skip']]
            symbols_to_fetch = [s for s in symbols_to_process
                                if not coverage_info[s]['should_skip']]

            # Update performance counters
            total_symbols_processed += len(symbols_to_process)
            total_symbols_skipped += len(symbols_with_coverage)
            total_symbols_fetched += len(symbols_to_fetch)

            # Log skipped symbols
            if symbols_with_coverage:
                print(f"✓ Skipping {len(symbols_with_coverage)} symbols with sufficient coverage:")
                for symbol in symbols_with_coverage:
                    info = coverage_info[symbol]
                    print(f"  - {symbol}: {info['record_count']} records "
                          f"({info['first_date']} to {info['last_date']})")

            # If all symbols have coverage, skip to next batch
            if not symbols_to_fetch:
                print("All symbols in this batch have sufficient coverage. Moving to next batch.")
                batch_delay = random.uniform(batch_delay_min, batch_delay_max)
                print(f"Waiting {batch_delay:.2f} seconds before next batch...")
                time.sleep(batch_delay)
                batch_number += 1
                processed_batches += 1
                if max_batches is not None and processed_batches >= max_batches:
                    print(f"Reached FINHISAAB_MAX_BATCHES={max_batches}. Stopping.")
                    break
                continue

            print(f"Will fetch data for {len(symbols_to_fetch)} symbols: {symbols_to_fetch}")

            # Try to fetch data for the whole batch in a single call
            batch_data = None
            try:
                print(f"Attempting batch fetch for symbols: {symbols_to_fetch}")
                batch_data = stocks(symbols_to_fetch, start=start_date, end=end_date)
            except Exception as e:
                print(f"Batch fetch failed; will fallback to per-symbol fetch. Error: {e}")
                batch_data = None

            for i, symbol in enumerate(symbols_to_fetch):
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
                        print(f"Fetching individually for symbol {symbol} due to unavailable batch data slice...")
                        symbol_df = stocks(symbol, start=start_date, end=end_date)

                    if symbol_df is None or symbol_df.empty:
                        print(f"No data found for {symbol} in this range. Recording failure.")
                        record_failed_interval(
                            symbol,
                            start_date,
                            end_date,
                            connection_string,
                            db_name,
                            reason="No data found or empty dataframe returned"
                        )
                        continue
                    else:
                        print(f"Retrieved {len(symbol_df)} records for {symbol}")
                except Exception as e:
                    print(f"An error occurred while fetching data for {symbol}: {e}")
                    record_failed_interval(
                        symbol,
                        start_date,
                        end_date,
                        connection_string,
                        db_name,
                        reason=f"Exception during fetch: {str(e)}"
                    )
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

                if not success:
                    record_failed_interval(
                        symbol,
                        start_date,
                        end_date,
                        connection_string,
                        db_name,
                        reason=f"Failed to save to MongoDB: {message}"
                    )

                # Delay between symbols to avoid overload
                if i < len(symbols_to_fetch) - 1:
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
        print("PERFORMANCE REPORT:")
        print(f"  Total symbols processed: {total_symbols_processed}")
        print(f"  Symbols skipped (coverage): {total_symbols_skipped}")
        print(f"  Symbols fetched: {total_symbols_fetched}")
        if total_symbols_processed > 0:
            skip_rate = (total_symbols_skipped / total_symbols_processed) * 100
            print(f"  Skip rate: {skip_rate:.1f}%")
        print(f"{'='*50}\n")
        print("All batches processed for the current run.")

    except (Exception, KeyboardInterrupt) as e:
        print(f"\n{'='*50}")
        print(f"CRITICAL: Script stopped abruptly due to: {e.__class__.__name__} - {e}")
        print("--- LAST PROCESSING STATE ---")
        print(f"Date Range:  {start_date} to {end_date}")
        print(f"Batch Size:  {batch_size}")
        print(f"Stopped at Batch Number: {batch_number}")
        print("You can resume by setting `batch_number` to the stopped batch.")
        print('='*50)


if __name__ == "__main__":
    main()
