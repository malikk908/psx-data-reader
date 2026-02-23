"""
Script to fetch and populate missing price data intervals for PSX stocks in MongoDB.
It reads from a generated JSON report (e.g., missing_data_report.json).
"""

import os
import json
import time
import random
import datetime
import pandas as pd
from psx import stocks
from psx.data_store import save_to_mongodb

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def load_missing_data_report(filepath="missing_data_report.json"):
    """
    Loads the JSON report containing the missing data ranges.
    Format: {"SYMBOL": [{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}]}
    """
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def main():
    # --- Configuration ---
    input_file = "missing_data_report.json"
    
    # MongoDB connection settings via environment variables
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://127.0.0.1:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")

    # Throttling controls
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "1"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "2"))
    batch_delay_min = float(os.getenv("FINHISAAB_BATCH_DELAY_MIN", "5"))
    batch_delay_max = float(os.getenv("FINHISAAB_BATCH_DELAY_MAX", "7"))

    print(f"Loading missing data from {input_file}...")
    missing_data = load_missing_data_report(input_file)
    
    if not missing_data:
        print("No missing data found or file could not be read. Exiting.")
        return

    # Restructure: Group by date range
    # {(start_date_str, end_date_str): [symbol1, symbol2, ...]}
    ranges_to_symbols = {}
    for symbol, ranges in missing_data.items():
        for r in ranges:
            key = (r['start'], r['end'])
            if key not in ranges_to_symbols:
                ranges_to_symbols[key] = []
            ranges_to_symbols[key].append(symbol)
            
    unique_ranges = list(ranges_to_symbols.keys())
    print(f"Found {len(unique_ranges)} unique missing date ranges across {len(missing_data)} symbols.")
    print("-" * 50)

    for i, (start_str, end_str) in enumerate(unique_ranges):
        symbols_to_process = ranges_to_symbols[(start_str, end_str)]
        
        # Convert strings to datetime.date objects for the psx module
        start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d").date()
        
        print(f"\nProcessing range [{i+1}/{len(unique_ranges)}]: {start_date} to {end_date} for {len(symbols_to_process)} symbols")
        
        try:
            # Fetch data directly from PSX for all symbols in this date range
            batch_data = stocks(symbols_to_process, start=start_date, end=end_date)
            
            if batch_data is None or (isinstance(batch_data, pd.DataFrame) and batch_data.empty):
                print(f"  -> No data found from PSX for this specific range for any symbols.")
                continue
                
            for symbol in symbols_to_process:
                # Resolve the DataFrame for this symbol
                symbol_df = None
                if isinstance(batch_data, dict) and symbol in batch_data:
                    symbol_df = batch_data[symbol]
                elif isinstance(batch_data, pd.DataFrame):
                    try:
                        index_names = list(batch_data.index.names or [])
                        if 'Ticker' in index_names:
                            symbol_df = batch_data.xs(symbol, level='Ticker')
                        else:
                            # If it's a single symbol fetch disguised as batch, or something else
                            # and it matches the single symbol we're expecting
                            if len(symbols_to_process) == 1:
                                symbol_df = batch_data
                    except Exception:
                        symbol_df = None
                
                if symbol_df is None or symbol_df.empty:
                    print(f"  -> [ {symbol} ] No data found in batch.")
                    continue
                
                # Save data to MongoDB (this does an upsert, safely handling overlaps)
                success, message = save_to_mongodb(
                    df=symbol_df,
                    symbol=symbol,
                    connection_string=connection_string,
                    db_name=db_name,
                    collection_name=collection_name
                )
                
                if not success:
                    print(f"  -> [ {symbol} ] Failed to save: {message}")
                else:
                    print(f"  -> [ {symbol} ] Saved {len(symbol_df)} records.")
            
        except Exception as e:
            print(f"  -> Error fetching or saving batch data: {e}")
        
        # Delay between different date ranges to avoid rate limits
        if i < len(unique_ranges) - 1:
            delay = random.uniform(batch_delay_min, batch_delay_max)
            print(f"Waiting {delay:.2f} seconds before next date range...")
            time.sleep(delay)

    print("\n" + "=" * 50)
    print("FINISHED FILLING MISSING DATA")
    print("=" * 50)

if __name__ == "__main__":
    main()
