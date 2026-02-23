import os
import argparse
import requests
from pymongo import MongoClient
import pandas as pd

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

def fetch_psx_symbols():
    url = "https://dps.psx.com.pk/symbols"
    print(f"Fetching symbols from {url} ...")
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_db_symbols(connection_string, db_name):
    client = MongoClient(connection_string)
    db = client[db_name]
    stocks_collection = db['stocks']
    
    symbols = stocks_collection.find({}, {'symbol': 1, '_id': 0})
    return {s['symbol'] for s in symbols}

def add_missing_symbols(connection_string, db_name, missing_symbols_data):
    if not missing_symbols_data:
        return

    client = MongoClient(connection_string)
    db = client[db_name]
    stocks_collection = db['stocks']
    
    # We may want to add basic fields found in the API
    # The API returns objects like:
    # {"symbol":"AKBLTFC6","name":"Askari Bank(TFC6)","sectorName":"BILLS AND BONDS","isETF":false,"isDebt":true}
    
    # Optional: we can add them to the database
    print(f"Inserting {len(missing_symbols_data)} new symbols into the database...")
    result = stocks_collection.insert_many(missing_symbols_data)
    print(f"Successfully inserted {len(result.inserted_ids)} symbols.")

def main():
    parser = argparse.ArgumentParser(description="Find and optionally sync missing PSX symbols to MongoDB.")
    parser.add_argument("--add", action="store_true", help="Add the missing symbols to the database")
    args = parser.parse_args()

    # MongoDB connection settings via environment variables
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")

    try:
        psx_data = fetch_psx_symbols()
    except Exception as e:
        print(f"Failed to fetch symbols from PSX: {e}")
        return

    psx_symbols_dict = {}
    for item in psx_data:
        if item.get('isDebt', False):
            continue
            
        name = item.get('name', '').strip()
        if not name or '(r)' in name.lower() or name == '()':
            continue
            
        psx_symbols_dict[item['symbol']] = item
    
    print("Connecting to MongoDB to get existing symbols...")
    try:
        db_symbols = get_db_symbols(connection_string, db_name)
    except Exception as e:
        print(f"Failed to connect to MongoDB: {e}")
        return

    missing_symbols = set(psx_symbols_dict.keys()) - db_symbols
    
    if not missing_symbols:
        print("No missing symbols found. The database is up to date.")
        return
        
    print(f"\nFound {len(missing_symbols)} missing symbols:")
    missing_data_to_insert = []
    
    # Print the missing symbols and aggregate data for insertion
    for sym in sorted(missing_symbols):
        print(f" - {sym} ({psx_symbols_dict[sym].get('name', 'N/A')})")
        missing_data_to_insert.append(psx_symbols_dict[sym])
    
    print(f"\nTotal missing symbols: {len(missing_symbols)}")
    
    if args.add:
        try:
            add_missing_symbols(connection_string, db_name, missing_data_to_insert)
        except Exception as e:
            print(f"Failed to insert missing symbols: {e}")
    else:
        print("\nRun with --add flag to insert these missing symbols into the database.")

if __name__ == "__main__":
    main()
