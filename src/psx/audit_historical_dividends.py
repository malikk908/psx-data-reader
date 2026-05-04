import os
import json
import time
import argparse
from datetime import datetime
from pymongo import MongoClient
import requests

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# Endpoint configuration
ENDPOINT_URL = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx/chartact"
REFERER_URL = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx"

def fetch_historical_dividends(symbol, session):
    """
    Fetches the full history for a specific symbol from scstrade.
    """
    print(f"Fetching history for: {symbol}...", end=" ", flush=True)
    
    # Use a current timestamp for cache-busting
    nd = int(time.time() * 1000)
    
    payload = {
        "par": symbol, 
        "_search": False, 
        "nd": nd, 
        "rows": 5000, 
        "page": 1, 
        "sidx": "", 
        "sord": "asc"
    }
    
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": REFERER_URL,
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = session.post(ENDPOINT_URL, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            records = data.get('d', []) if isinstance(data, dict) else data
            print(f"Found {len(records)} records.")
            return records
        else:
            print(f"Failed (Status Code: {response.status_code})")
            return []
    except Exception as e:
        print(f"Error: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="Audit historical dividends for PSX stocks")
    parser.add_argument("--start", type=int, default=1, help="Start index (1-based)")
    parser.add_argument("--end", type=int, default=20, help="End index (inclusive)")
    parser.add_argument("--out", type=str, default="historical_dividends_audit.json", help="Output JSON file")
    args = parser.parse_args()

    # MongoDB connection
    connection_string = os.getenv("FINHISAAB_PRIMARY_DB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_PRIMARY_DB_NAME", "finhisaab")
    
    print("=" * 80)
    print(f"HISTORICAL DIVIDEND AUDIT (Stocks {args.start} to {args.end})")
    print("=" * 80)
    
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[db_name]
        print(f"Connected to MongoDB database: {db_name}")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")
        return

    # Calculate skip and limit for pagination
    # Note: start is 1-based, so skip = start - 1
    skip_count = max(0, args.start - 1)
    limit_count = max(1, args.end - skip_count)

    # Fetch stocks sorted by marketCap descending
    stocks_cursor = db['stocks'].find(
        {"isActive": True}, 
        {"symbol": 1, "marketCap": 1, "_id": 0}
    ).sort("marketCap", -1).skip(skip_count).limit(limit_count)

    stocks = list(stocks_cursor)
    print(f"Retrieved {len(stocks)} stocks from database to process.")
    print("-" * 80)

    # Load existing JSON data if the file exists to allow appending
    output_data = []
    if os.path.exists(args.out):
        try:
            with open(args.out, 'r') as f:
                output_data = json.load(f)
            print(f"Loaded {len(output_data)} existing records from {args.out}")
        except json.JSONDecodeError:
            print(f"Warning: Could not parse {args.out}. Starting fresh.")
            output_data = []

    # Use a session for better connection pooling
    session = requests.Session()
    
    new_records_count = 0
    
    for i, stock in enumerate(stocks, 1):
        symbol = stock.get("symbol")
        if not symbol:
            continue
            
        print(f"[{i}/{len(stocks)}] ", end="")
        records = fetch_historical_dividends(symbol, session)
        
        if records:
            # Add metadata to each record to know which symbol it belongs to
            # (although the API response usually includes 'company_code', this is safer)
            for r in records:
                r['_scraped_symbol'] = symbol
                r['_scraped_at'] = datetime.now().isoformat()
            
            output_data.extend(records)
            new_records_count += len(records)
            
        # Polite delay to avoid hammering the server
        time.sleep(1.5)

    # Save all data back to the JSON file
    print("-" * 80)
    print(f"Saving {len(output_data)} total records (added {new_records_count} new) to {args.out}...")
    
    with open(args.out, 'w') as f:
        json.dump(output_data, f, indent=2)
        
    print("Done!")

if __name__ == "__main__":
    main()
