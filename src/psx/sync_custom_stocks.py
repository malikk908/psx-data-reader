"""
Script to fetch historical PSX stock price data for a custom defined list of symbols
and a custom start date, then optionally save them to MongoDB.
Supports both in-script configuration and CLI overrides, including dry-run mode.
"""

import os
import random
import time
import datetime
import argparse
import pandas as pd
from psx import stocks
from psx.data_store import save_to_mongodb, connect_to_mongodb

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ==============================================================================
# IN-SCRIPT CONFIGURATION
# Change these values to set default targets when running without CLI flags.
# ==============================================================================
# The list of stock symbols you want to fetch:
DEFAULT_SYMBOLS = ["SYS", "MEBL", "ENGRO", "OGDC", "HUBC"]

# The start date for the historical data lookup:
DEFAULT_START_DATE = datetime.date(2023, 1, 1)

# Set to True by default if you want to run safely without writing to DB:
DEFAULT_DRY_RUN = False
# ==============================================================================


def test_mongo_connectivity(connection_string: str, db_name: str) -> bool:
    """
    Perform a quick connectivity test to MongoDB.
    """
    try:
        from pymongo import MongoClient
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        return True
    except Exception as e:
        print(f"[-] MongoDB connectivity check FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Sync selected PSX symbols from a custom start date to MongoDB."
    )
    parser.add_argument(
        "--symbols",
        type=str,
        nargs="+",
        help="List of stock symbols separated by spaces (e.g. SYS MEBL ENGRO). Overrides DEFAULT_SYMBOLS.",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (e.g. 2023-01-01). Overrides DEFAULT_START_DATE.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=None,
        help="Fetch and print data details from PSX but DO NOT save to MongoDB.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        default=None,
        help="Force write data to MongoDB (disables dry-run).",
    )
    args = parser.parse_args()

    # Determine symbols
    symbols = args.symbols if args.symbols is not None else DEFAULT_SYMBOLS
    if not symbols:
        print("[-] Error: No symbols defined. Please specify symbols in script or via --symbols.")
        return

    # Determine start and end dates
    if args.start_date:
        try:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"[-] Error: Invalid date format '{args.start_date}'. Use YYYY-MM-DD.")
            return
    else:
        start_date = DEFAULT_START_DATE

    end_date = datetime.date.today()

    # Determine dry run status
    # Command line args have highest priority, then DEFAULT_DRY_RUN configuration
    dry_run = DEFAULT_DRY_RUN
    if args.dry_run is True:
        dry_run = True
    elif args.write is True:
        dry_run = False

    print("=" * 60)
    print("           PSX CUSTOM STOCKS SYNC PIPELINE")
    print("=" * 60)
    print(f"[*] Target Symbols: {symbols}")
    print(f"[*] Date Range:     {start_date} to {end_date}")
    print(f"[*] Mode:           {'DRY RUN (No database writes)' if dry_run else 'WRITE TO DATABASE'}")
    print("=" * 60)

    # Database configuration (only loaded/tested if NOT a dry run)
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    if not dry_run:
        print(f"[*] Testing connection to MongoDB ({connection_string})...")
        if not test_mongo_connectivity(connection_string, db_name):
            print("[-] MongoDB is unreachable. Exiting. Run with --dry-run if you just want to test fetching data.")
            return
        print("[+] MongoDB connectivity verified successfully.")

    # Throttling controls
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1.0"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2.0"))

    total_symbols = len(symbols)
    successful_syncs = []
    failed_syncs = []

    for idx, symbol in enumerate(symbols):
        print(f"\n[{idx + 1}/{total_symbols}] Processing symbol: {symbol} ...")
        
        try:
            # Fetch data from PSX using the core reader library
            df = stocks(symbol, start=start_date, end=end_date)
            
            if df is None or df.empty:
                print(f"  [-] No data returned from PSX for {symbol} in the requested range.")
                failed_syncs.append((symbol, "No data returned / Empty DataFrame"))
                continue
                
            print(f"  [+] Successfully fetched {len(df)} records.")
            print(f"      Columns: {list(df.columns)}")
            print(f"      First Date: {df.index.min().strftime('%Y-%m-%d')}")
            print(f"      Last Date:  {df.index.max().strftime('%Y-%m-%d')}")

            # Dry Run vs Database Save
            if dry_run:
                print("  [DRY RUN] Skipping MongoDB write operations.")
                successful_syncs.append((symbol, f"Fetched {len(df)} records (Dry Run)"))
            else:
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
    print(f"Total processed: {total_symbols}")
    print(f"Successful:      {len(successful_syncs)}")
    print(f"Failed:          {len(failed_syncs)}")
    
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
