"""
Script to fetch and populate missing price data intervals for PSX stocks in MongoDB.
It reads from a generated JSON report (e.g., missing_data_report.json).
"""

import os
import json
import time
import random
import datetime
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

    symbols = list(missing_data.keys())
    print(f"Found {len(symbols)} symbols with missing data to process.")
    print("-" * 50)

    for i, symbol in enumerate(symbols):
        ranges = missing_data[symbol]
        print(f"\nProcessing symbol: {symbol} ({len(ranges)} missing ranges)")
        
        for r_idx, r in enumerate(ranges):
            start_str = r['start']
            end_str = r['end']
            
            # Convert strings to datetime.date objects for the psx module
            start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d").date()
            
            print(f"  Fetching [{r_idx+1}/{len(ranges)}]: {start_date} to {end_date}...")
            
            try:
                # Fetch data directly from PSX
                symbol_df = stocks(symbol, start=start_date, end=end_date)
                
                if symbol_df is None or symbol_df.empty:
                    print(f"  -> No data found from PSX for this specific range.")
                else:
                    print(f"  -> Retrieved {len(symbol_df)} records. Saving to MongoDB...")
                    
                    # Save data to MongoDB (this does an upsert, safely handling overlaps)
                    success, message = save_to_mongodb(
                        df=symbol_df,
                        symbol=symbol,
                        connection_string=connection_string,
                        db_name=db_name,
                        collection_name=collection_name
                    )
                    
                    if not success:
                        print(f"  -> Failed to save: {message}")
                    else:
                        print(f"  -> Successfully saved.")
                
            except Exception as e:
                print(f"  -> Error fetching or saving data: {e}")
            
            # Small delay between individual date ranges for the same symbol to avoid rate limits
            if r_idx < len(ranges) - 1:
                time.sleep(1)

        # Longer delay between different symbols to avoid overload
        if i < len(symbols) - 1:
            delay = random.uniform(symbol_delay_min, symbol_delay_max)
            print(f"Waiting {delay:.2f} seconds before next symbol...")
            time.sleep(delay)

    print("\n" + "=" * 50)
    print("FINISHED FILLING MISSING DATA")
    print("=" * 50)

if __name__ == "__main__":
    main()
