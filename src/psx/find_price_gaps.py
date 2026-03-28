import os
import argparse
import json
from datetime import datetime
from pymongo import MongoClient

def connect_to_mongodb(connection_string, db_name):
    client = MongoClient(connection_string)
    db = client[db_name]
    return client, db

def main():
    parser = argparse.ArgumentParser(description="Find missing price gaps for stocks.")
    parser.add_argument("--start", type=int, default=1, help="Start index (1-based) of the stocks to process.")
    parser.add_argument("--end", type=int, default=None, help="End index (1-based) of the stocks.")
    parser.add_argument("--out", type=str, default="price_gaps.json", help="Output JSON file name.")
    args = parser.parse_args()

    # Connection settings from env or default
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_COLLECTION", "stockpricehistories")
    
    print(f"Connecting to MongoDB at {connection_string}, DB: {db_name}")

    client, db = connect_to_mongodb(connection_string, db_name)
    history_coll = db[collection_name]
    stocks_coll = db["stocks"]

    print(f"Fetching distinct trading dates from {collection_name}...")
    # Get all distinct trading dates as the master PSX calendar
    all_dates = history_coll.distinct("date")
    if not all_dates:
        print("No dates found in the database. Exiting.")
        return

    # Ensure all dates are comparable types and sorted
    master_dates = sorted([d if isinstance(d, datetime) else d for d in all_dates])
    master_date_to_index = {d: i for i, d in enumerate(master_dates)}
    print(f"Found {len(master_dates)} distinct trading dates. (from {master_dates[0].strftime('%Y-%m-%d')} to {master_dates[-1].strftime('%Y-%m-%d')})")

    # Fetch symbols sorted by marketCap descending
    print("Fetching stocks sorted by marketCap (descending)...")
    symbols_cursor = stocks_coll.find({}, {'symbol': 1, 'marketCap': 1, '_id': 0}).sort('marketCap', -1)
    all_symbols = [s['symbol'] for s in symbols_cursor]
    
    total_symbols = len(all_symbols)
    start_idx = max(0, args.start - 1)
    end_idx = args.end if args.end is not None else total_symbols
    
    symbols_to_process = all_symbols[start_idx:end_idx]
    print(f"Processing stocks from index {start_idx + 1} to {min(end_idx, total_symbols)} (Total: {len(symbols_to_process)} stocks)")

    results = {}
    
    for count, symbol in enumerate(symbols_to_process, 1):
        if count % 10 == 0:
            print(f"Processing {count}/{len(symbols_to_process)}: {symbol}...")
            
        symbol_docs = history_coll.find({"symbol": symbol}, {"date": 1, "_id": 0})
        symbol_dates_raw = [doc["date"] for doc in symbol_docs if "date" in doc]
        
        if not symbol_dates_raw:
            continue
            
        symbol_dates = sorted([d if isinstance(d, datetime) else d for d in symbol_dates_raw])
        symbol_min = symbol_dates[0]
        symbol_max = symbol_dates[-1]
        
        # Determine the subset of expected days from the master calendar
        min_idx = master_date_to_index.get(symbol_min)
        max_idx = master_date_to_index.get(symbol_max)
        
        if min_idx is None or max_idx is None:
            # Fallback if date wasn't matching up cleanly in master index (shouldn't happen)
            continue
            
        expected_indices = set(range(min_idx, max_idx + 1))
        actual_indices = set([master_date_to_index[d] for d in symbol_dates if d in master_date_to_index])
        
        missing_indices = sorted(list(expected_indices - actual_indices))
        
        if not missing_indices:
            continue
            
        # Group missing indices into contiguous gaps
        gaps = []
        current_gap = [missing_indices[0]]
        
        for i in range(1, len(missing_indices)):
            if missing_indices[i] == current_gap[-1] + 1:
                current_gap.append(missing_indices[i])
            else:
                gaps.append(current_gap)
                current_gap = [missing_indices[i]]
        if current_gap:
            gaps.append(current_gap)
            
        # Format the gaps
        symbol_gaps_formatted = []
        for gap in gaps:
            start_date = master_dates[gap[0]].strftime('%Y-%m-%d')
            end_date = master_dates[gap[-1]].strftime('%Y-%m-%d')
            last_actual = "N/A"
            if gap[0] > 0:
                last_actual = master_dates[gap[0] - 1].strftime('%Y-%m-%d')
            
            symbol_gaps_formatted.append({
                "start": start_date,
                "end": end_date,
                "missing_trading_days": len(gap),
                "last_actual_date": last_actual
            })
            
        if symbol_gaps_formatted:
            results[symbol] = symbol_gaps_formatted

    print(f"\nFound gaps for {len(results)} stocks out of {len(symbols_to_process)} processed.")
    
    with open(args.out, "w") as f:
        json.dump(results, f, indent=4)
        
    print(f"Results saved to {args.out}")

if __name__ == "__main__":
    main()
