import os
import argparse
import json
import time
import random
from datetime import datetime
from psx import stocks
from psx.data_store import save_to_mongodb
from pymongo import MongoClient

def connect_to_mongodb(connection_string, db_name):
    client = MongoClient(connection_string)
    db = client[db_name]
    return client, db

def load_gaps_file(filepath):
    if not os.path.exists(filepath):
        print(f"Error: The file {filepath} does not exist.")
        return None
    with open(filepath, 'r') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            print(f"Error: The file {filepath} is not valid JSON.")
            return None

def save_gaps_file(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4)

def parse_date(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def main():
    parser = argparse.ArgumentParser(description="Fetch missing price gaps for stocks.")
    parser.add_argument("--file", type=str, default="price_gaps.json", help="Path to the JSON gaps file.")
    parser.add_argument("--min-days", type=int, default=7, help="Minimum missing trading days to warrant a fetch.")
    parser.add_argument("--min-year", type=int, default=2010, help="Minimum year to consider for fetching a gap.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of stocks to process.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate fetching without saving to DB or updating JSON.")
    args = parser.parse_args()

    # Connection settings from env or default
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")
    
    if args.dry_run:
        print("\n*** RUNNING IN DRY-RUN MODE: No database or file changes will occur ***\n")
    else:
        print(f"Connecting to MongoDB at {connection_string}, DB: {db_name}")

    gaps_data = load_gaps_file(args.file)
    if gaps_data is None:
        return

    if not args.dry_run:
        client, db = connect_to_mongodb(connection_string, db_name)

    # Optional throttling controls, similar to the cron job
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2"))

    symbols = list(gaps_data.keys())
    
    if args.limit and args.limit > 0:
        symbols = symbols[:args.limit]
        print(f"Processing up to {args.limit} stocks...")
    else:
        print(f"Processing all {len(symbols)} stocks...")

    processed_count = 0
    symbols_fully_resolved = []

    for symbol in symbols:
        print(f"\n{'='*50}")
        print(f"Processing {symbol}...")
        
        gaps = gaps_data[symbol]
        valid_gaps = []
        
        # Filter gaps based on criteria
        for gap in gaps:
            start_year = parse_date(gap['start']).year
            if gap['missing_trading_days'] >= args.min_days and start_year >= args.min_year:
                valid_gaps.append(gap)
            else:
                reason = "days < min-days" if gap['missing_trading_days'] < args.min_days else "start year < min-year"
                print(f"  [Skipping] Gap {gap['start']} to {gap['end']} ({gap['missing_trading_days']} days): {reason}")

        if not valid_gaps:
             print(f"  => No valid gaps meeting criteria for {symbol}.")
             symbols_fully_resolved.append(symbol)
             continue
             
        symbol_success = True
        
        for idx, gap in enumerate(valid_gaps):
            start_date = parse_date(gap['start'])
            end_date = parse_date(gap['end'])
            
            print(f"\n  Fetching gap #{idx+1}/{len(valid_gaps)} for {symbol} ({start_date} to {end_date})")
            
            try:
                if args.dry_run:
                    print(f"  [DRY-RUN] Would fetch: stocks('{symbol}', start={start_date}, end={end_date})")
                    print(f"  [DRY-RUN] Would save data to MongoDB collection: {collection_name}")
                else:
                    symbol_df = stocks(symbol, start=start_date, end=end_date)
                    
                    if symbol_df is None or symbol_df.empty:
                        print(f"  No data returned by API for {symbol} between {start_date} and {end_date}.")
                        # Depending on definition of success, this could mean the PSX API has no data for that gap.
                        # We will consider it 'processed' so we don't continually loop empty sections.
                        continue
                    
                    print(f"  Retrieved {len(symbol_df)} records. Saving to DB...")
                    success, message = save_to_mongodb(
                        df=symbol_df,
                        symbol=symbol,
                        connection_string=connection_string,
                        db_name=db_name,
                        collection_name=collection_name
                    )
                    
                    print(f"  MongoDB Save Result: {'Success' if success else 'Failed'} - {message}")
                    if not success:
                        symbol_success = False

                # Delay between gap requests to be nice to the PSX API
                if idx < len(valid_gaps) - 1:
                    delay = random.uniform(symbol_delay_min, symbol_delay_max)
                    time.sleep(delay)
                    
            except Exception as e:
                print(f"  Error fetching or saving gap data for {symbol}: {e}")
                symbol_success = False

        if symbol_success:
            symbols_fully_resolved.append(symbol)
            
        processed_count += 1
        
        # Delay between stocks
        if processed_count < len(symbols):
            delay = random.uniform(symbol_delay_min, symbol_delay_max)
            time.sleep(delay)
            
    print(f"\n{'='*50}")
    print(f"Finished processing batch of {len(symbols)} stocks.")

    # Only update the JSON file if not a dry-run
    if not args.dry_run and symbols_fully_resolved:
        print(f"Removing {len(symbols_fully_resolved)} resolved stock(s) from {args.file}...")
        original_count = len(gaps_data)
        for symbol in symbols_fully_resolved:
            if symbol in gaps_data:
                del gaps_data[symbol]
                
        save_gaps_file(args.file, gaps_data)
        print(f"Updated {args.file}: went from {original_count} to {len(gaps_data)} stocks remaining.")
    elif args.dry_run:
        print(f"[DRY-RUN] Would remove {len(symbols_fully_resolved)} resolved stock(s) from {args.file}.")

if __name__ == "__main__":
    main()
