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
import math
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
    symbol_delay_min = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MIN", "0.5"))
    symbol_delay_max = float(os.getenv("FINHISAAB_SYMBOL_DELAY_MAX", "1.0"))
    batch_delay_min = float(os.getenv("FINHISAAB_BATCH_DELAY_MIN", "1.0"))
    batch_delay_max = float(os.getenv("FINHISAAB_BATCH_DELAY_MAX", "3.0"))
    
    # Sub-batching setting for large lists of symbols
    fetch_batch_size = int(os.getenv("FINHISAAB_BATCH_SIZE", "10"))

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
    
    # Sort the unique ranges by duration in descending order (longest first)
    def get_duration(date_range):
        start_d = datetime.datetime.strptime(date_range[0], "%Y-%m-%d").date()
        end_d = datetime.datetime.strptime(date_range[1], "%Y-%m-%d").date()
        return (end_d - start_d).days

    unique_ranges.sort(key=get_duration, reverse=True)

    print(f"Found {len(unique_ranges)} unique missing date ranges across {len(missing_data)} symbols.")
    print("-" * 50)

    for i, (start_str, end_str) in enumerate(unique_ranges):
        symbols_to_process = ranges_to_symbols[(start_str, end_str)]
        
        # Convert strings to datetime.date objects for the psx module
        start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d").date()
        
        print(f"\nProcessing range [{i+1}/{len(unique_ranges)}]: {start_date} to {end_date} for {len(symbols_to_process)} symbols")
        
        # Sub-divide symbols if the list is too large to fetch at once
        num_sub_batches = math.ceil(len(symbols_to_process) / fetch_batch_size)
        
        range_fully_successful = True
        
        for sub_batch_idx in range(num_sub_batches):
            start_idx = sub_batch_idx * fetch_batch_size
            end_idx = min(start_idx + fetch_batch_size, len(symbols_to_process))
            current_sub_batch = symbols_to_process[start_idx:end_idx]
            
            if num_sub_batches > 1:
                print(f"  -> Sub-batch [{sub_batch_idx+1}/{num_sub_batches}] ({len(current_sub_batch)} symbols)...")
            
            try:
                # Fetch data directly from PSX for this sub-batch
                batch_data = stocks(current_sub_batch, start=start_date, end=end_date)
                
                if batch_data is None or (isinstance(batch_data, pd.DataFrame) and batch_data.empty):
                    print(f"    -> No data found from PSX for this specific sub-batch.")
                    # We might still continue with other sub-batches, but we mark this range 
                    # as basically 'done' since PSX returned nothing.
                    continue
                    
                for symbol in current_sub_batch:
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
                                if len(current_sub_batch) == 1:
                                    symbol_df = batch_data
                        except Exception:
                            symbol_df = None
                    
                    if symbol_df is None or symbol_df.empty:
                        print(f"    -> [ {symbol} ] No data found in batch.")
                        continue
                    
                    success, message = save_to_mongodb(
                        df=symbol_df,
                        symbol=symbol,
                        connection_string=connection_string,
                        db_name=db_name,
                        collection_name=collection_name
                    )
                    
                    if not success:
                        print(f"    -> [ {symbol} ] Failed to save: {message}")
                        range_fully_successful = False
                    else:
                        print(f"    -> [ {symbol} ] Saved {len(symbol_df)} records.")
                
            except Exception as e:
                print(f"    -> Error fetching or saving sub-batch data: {e}")
                range_fully_successful = False
                
            # Delay between sub-batches
            if sub_batch_idx < num_sub_batches - 1:
                time.sleep(random.uniform(symbol_delay_min, symbol_delay_max))
        
        # If we successfully processed (or verified empty) this entire date range for all symbols,
        # remove it from the report file so we don't repeat it if the script crashes later.
        if range_fully_successful:
            for symbol in symbols_to_process:
                if symbol in missing_data:
                    # Filter out this specific range
                    missing_data[symbol] = [
                        r for r in missing_data[symbol] 
                        if not (r['start'] == start_str and r['end'] == end_str)
                    ]
                    # If symbol has no more missing data, remove it from dict
                    if not missing_data[symbol]:
                        del missing_data[symbol]
            
            # Save the updated progress exactly back to the file
            try:
                with open(input_file, 'w') as f:
                    json.dump(missing_data, f, indent=4)
                print(f"  -> Progress saved. {start_str} to {end_str} removed from report.")
            except Exception as e:
                print(f"  -> Warning: Failed to update progress in {input_file}: {e}")
        
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
