#!/usr/bin/env python3
"""
Script to query MongoDB 'stocks' collection by custom criteria (e.g., createdAt)
and fetch/store stock price history from a specified date onwards.
Supports batching options to run target slices of symbols.
"""

import os
import re
import json
import time
import random
import datetime
import argparse
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson import json_util

from psx import stocks
from psx.data_store import save_to_mongodb

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def test_mongo_connectivity(connection_string: str, db_name: str) -> bool:
    """
    Perform a quick connectivity test to MongoDB.
    """
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        return True
    except Exception as e:
        print(f"[-] MongoDB connectivity check FAILED: {e}")
        return False

def parse_iso_datetime(val):
    if not isinstance(val, str):
        return val
    # Match YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
        try:
            return datetime.datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            pass
    # Match YYYY-MM-DDTHH:MM:SS (optional timezone/milliseconds)
    if re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", val):
        try:
            # Strip Z if present
            cleaned = val.rstrip('Z')
            if '.' in cleaned:
                parts = cleaned.split('.')
                dt = datetime.datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%S")
                return dt
            return datetime.datetime.strptime(cleaned, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    return val

def convert_dates_in_query(q):
    if isinstance(q, dict):
        return {k: convert_dates_in_query(v) for k, v in q.items()}
    elif isinstance(q, list):
        return [convert_dates_in_query(v) for v in q]
    else:
        return parse_iso_datetime(q)

def get_matching_symbols(connection_string, db_name, query_filter):
    """
    Queries the stocks collection and returns all matching symbols sorted alphabetically.
    """
    client = MongoClient(connection_string)
    db = client[db_name]
    stocks_collection = db['stocks']

    print(f"[*] Querying 'stocks' collection with filter: {query_filter}")
    cursor = stocks_collection.find(query_filter, {'symbol': 1, '_id': 0})
    
    symbols = sorted([doc['symbol'] for doc in cursor if 'symbol' in doc])
    client.close()
    return symbols

def main():
    parser = argparse.ArgumentParser(
        description="Query stocks collection and fetch/store price history from a specified date onwards."
    )
    parser.add_argument(
        "--created-after",
        type=str,
        help="Filter stocks created on or after this date/datetime (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)."
    )
    parser.add_argument(
        "--query",
        type=str,
        help="Raw JSON query filter to apply to the stocks collection. E.g., '{\"isETF\": false}'."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date to fetch prices from (format: YYYY-MM-DD). Defaults to 30 days ago."
    )
    parser.add_argument(
        "--batch-number",
        type=int,
        default=1,
        help="Batch number to fetch (1-indexed)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Number of stocks to process per batch (max 25 by default)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Query symbols and print them without executing fetches or writes."
    )

    args = parser.parse_args()

    # MongoDB connection settings via environment variables
    # Default to the correct host 192.168.0.131 as verified from other files
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    # 1. Determine the start date for price fetching
    if args.start_date:
        try:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[-] Error: Invalid date format '{args.start_date}' for --start-date. Use YYYY-MM-DD.")
            return
    else:
        # Default to 30 days ago
        start_date = (datetime.date.today() - datetime.timedelta(days=30))

    end_date = datetime.date.today()

    # 2. Build the query filter for the stocks collection
    query_filter = {}

    if args.query:
        try:
            # Use json_util to support Mongo Extended JSON
            parsed_query = json_util.loads(args.query)
            query_filter.update(convert_dates_in_query(parsed_query))
        except Exception as e:
            print(f"[-] Error parsing custom --query JSON: {e}")
            return

    if args.created_after:
        try:
            parsed_created_after = parse_iso_datetime(args.created_after)
            if isinstance(parsed_created_after, str):
                # If it didn't parse to datetime, try standard conversion
                parsed_created_after = datetime.datetime.strptime(args.created_after, "%Y-%m-%d")
        except ValueError:
            print(f"[-] Error: Invalid date/datetime format '{args.created_after}' for --created-after. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.")
            return

        # Query handles both BSON Date and string storage if present
        created_filter = {
            "$or": [
                {"createdAt": {"$gte": parsed_created_after}},
                {"createdAt": {"$gte": args.created_after}}
            ]
        }
        
        # Merge filters
        if query_filter:
            query_filter = {"$and": [query_filter, created_filter]}
        else:
            query_filter = created_filter

    if not args.query and not args.created_after:
        print("[-] Error: You must provide either --created-after or a custom --query to filter stocks.")
        parser.print_help()
        return

    # Check connection if writing to database
    if not args.dry_run:
        print(f"[*] Testing connection to MongoDB...")
        if not test_mongo_connectivity(connection_string, db_name):
            print("[-] MongoDB is unreachable. Exiting.")
            return
        print("[+] MongoDB connectivity verified successfully.")

    # 3. Retrieve matching symbols
    try:
        all_symbols = get_matching_symbols(connection_string, db_name, query_filter)
    except Exception as e:
        print(f"[-] Failed to retrieve symbols from database: {e}")
        return

    if not all_symbols:
        print("[*] No symbols found matching the specified criteria.")
        return

    # Apply batching
    total_matching = len(all_symbols)
    batch_size = max(1, min(25, args.batch_size))  # Max 25 per user requirements
    batch_number = max(1, args.batch_number)
    
    skip_amount = (batch_number - 1) * batch_size
    symbols = all_symbols[skip_amount : skip_amount + batch_size]

    if not symbols:
        print(f"[*] Batch {batch_number} (size {batch_size}) contains no symbols. Total matching: {total_matching}.")
        return

    print("=" * 60)
    print(f"[*] Total matching symbols found: {total_matching}")
    print(f"[*] Processing Batch {batch_number} (size {batch_size}): {symbols}")
    print(f"[*] Fetching historical prices from: {start_date} to {end_date}")
    print(f"[*] Mode: {'DRY RUN' if args.dry_run else 'WRITE TO DATABASE'}")
    print("=" * 60)

    if args.dry_run:
        print("[DRY RUN] Done. No API requests or database updates were made.")
        return

    # Throttling defaults
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1.0"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2.0"))

    successful_syncs = []
    failed_syncs = []

    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols):
        print(f"\n[{idx + 1}/{total_symbols}] Fetching data for symbol: {symbol} ...")
        
        try:
            # Fetch data from PSX using the reader library
            df = stocks(symbol, start=start_date, end=end_date)
            
            if df is None or df.empty:
                print(f"  [-] No data returned from PSX for {symbol} in the requested range.")
                failed_syncs.append((symbol, "No data / Empty DataFrame"))
                continue
                
            print(f"  [+] Successfully fetched {len(df)} records.")
            print(f"      First Date: {df.index.min().strftime('%Y-%m-%d')}")
            print(f"      Last Date:  {df.index.max().strftime('%Y-%m-%d')}")

            print(f"  [*] Saving {len(df)} records to MongoDB...")
            success, message = save_to_mongodb(
                df=df,
                symbol=symbol,
                connection_string=connection_string,
                db_name=db_name,
                collection_name=collection_name
            )
            if success:
                print(f"  [+] Success: {message}")
                successful_syncs.append((symbol, f"Saved to DB: {message}"))
            else:
                print(f"  [-] Failed to save: {message}")
                failed_syncs.append((symbol, f"DB Save Error: {message}"))

        except Exception as e:
            print(f"  [-] Error occurred while processing {symbol}: {e}")
            failed_syncs.append((symbol, f"Error: {str(e)}"))

        # Throttle between symbols to prevent rate limits / IP block
        if idx < total_symbols - 1:
            delay = random.uniform(symbol_delay_min, symbol_delay_max)
            print(f"  [*] Throttling: waiting {delay:.2f} seconds before next symbol...")
            time.sleep(delay)

    # Final summary report
    print("\n" + "=" * 60)
    print("                      EXECUTION SUMMARY")
    print("=" * 60)
    print(f"Total processed in this batch: {total_symbols}")
    print(f"Successful:                    {len(successful_syncs)}")
    print(f"Failed:                        {len(failed_syncs)}")
    
    if successful_syncs:
        print("\nSuccessful Syncs:")
        for sym, msg in successful_syncs:
            print(f"  ✓ {sym:8} -> {msg}")
            
    if failed_syncs:
        print("\nFailed / Skipped Syncs:")
        for sym, reason in failed_syncs:
            print(f"  ✗ {sym:8} -> {reason}")
    print("=" * 60)

if __name__ == "__main__":
    main()
