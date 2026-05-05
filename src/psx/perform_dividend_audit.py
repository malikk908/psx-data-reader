import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any
from pymongo import MongoClient
from psx.dividend_scraper import DividendScraper

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def connect_to_db():
    connection_string = os.getenv("FINHISAAB_PRIMARY_DB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_PRIMARY_DB_NAME", "finhisaab")
    client = MongoClient(connection_string)
    return client[db_name]

def run_audit(json_file_path: str, output_file_path: str, day_tolerance: int = 0, amount_tolerance: float = 0.0, has_face_value: bool = False):
    db = connect_to_db()
    scraper = DividendScraper()
    
    # 1. Audit stocks with missing faceValue
    logger.info("Auditing stocks for missing faceValue...")
    stocks_collection = db['stocks']
    all_stocks = list(stocks_collection.find({"isActive": True}, {"symbol": 1, "faceValue": 1, "name": 1}))
    
    symbols_with_face_value = []
    symbols_without_face_value = []
    face_values = {}
    
    for stock in all_stocks:
        sym = stock.get('symbol')
        fv = stock.get('faceValue')
        if fv is None:
            symbols_without_face_value.append({
                "symbol": sym,
                "name": stock.get('name')
            })
            face_values[sym] = 10.0
        else:
            symbols_with_face_value.append({
                "symbol": sym,
                "name": stock.get('name'),
                "faceValue": float(fv)
            })
            face_values[sym] = float(fv)

    # 2. Load JSON records
    logger.info(f"Loading JSON records from {json_file_path}...")
    if not os.path.exists(json_file_path):
        logger.error(f"File not found: {json_file_path}")
        return

    with open(json_file_path, 'r') as f:
        json_records = json.load(f)

    # 3. Load DB dividend announcements
    logger.info("Loading dividend announcements from DB...")
    dividend_collection = db['dividendannouncements']
    db_dividends = list(dividend_collection.find({}, {"symbol": 1, "exDate": 1, "amountPerShare": 1}))
    
    # Organize DB records for quick lookup: { symbol: { date_obj: amount } }
    db_lookup = {}
    for div in db_dividends:
        sym = div.get('symbol')
        ex_date = div.get('exDate')
        
        # Parse/Normalize date to datetime object (midnight)
        if isinstance(ex_date, datetime):
            ex_date_obj = ex_date.replace(hour=0, minute=0, second=0, microsecond=0)
        elif isinstance(ex_date, str):
            try:
                # Handle ISO format strings
                ex_date_obj = datetime.fromisoformat(ex_date.replace('Z', '+00:00')).replace(hour=0, minute=0, second=0, microsecond=0)
            except ValueError:
                continue
        else:
            continue
            
        amount = div.get('amountPerShare', 0)
        if sym not in db_lookup:
            db_lookup[sym] = {}
        db_lookup[sym][ex_date_obj] = amount

    # 4. Compare
    logger.info(f"Comparing records (Mode: {'Has Face Value' if has_face_value else 'Missing Face Value'})...")
    missing_in_db = []
    discrepancies = []
    processed_count = 0
    skipped_count = 0

    # Identify which symbols to audit
    if has_face_value:
        symbols_to_audit = {s['symbol'] for s in symbols_with_face_value}
    else:
        symbols_to_audit = {s['symbol'] for s in symbols_without_face_value}
        
    logger.info(f"Symbols to audit: {len(symbols_to_audit)}")

    for record in json_records:
        symbol = record.get('company_code') or record.get('_scraped_symbol')
        
        # SKIP stocks that HAVE a faceValue in DB
        if not symbol or symbol not in symbols_to_audit:
            continue

        dividend_str = record.get('bm_dividend', '').strip()
        date_str_raw = record.get('bm_bc_exp', '').strip()

        if not date_str_raw or not dividend_str:
            skipped_count += 1
            continue

        # Parse date
        ex_date_obj = scraper.parse_date(date_str_raw)
        if not ex_date_obj:
            skipped_count += 1
            continue
        
        ex_date_iso = ex_date_obj.strftime("%Y-%m-%d")
        
        # Parse percentage and calculate amount
        percentage = scraper._parse_percentage(dividend_str.split('(')[0])
        if percentage == 0:
            skipped_count += 1
            continue
            
        # Use either actual face value or assumed 10.0
        # Wait, user instruction was to assume 10 for calculation? 
        # But if we are auditing "Has Face Value" stocks, maybe we should use the actual one?
        # Actually, let's keep it flexible. If has_face_value is True, use actual. If False, use 10.
        calc_face_value = face_values.get(symbol, 10.0)
        expected_amount = round((percentage / 100.0) * calc_face_value, 4)
        
        actual_db_face_value = face_values.get(symbol)

        # Check in DB with date tolerance
        symbol_db_records = db_lookup.get(symbol, {})
        
        # Look for a record within tolerance
        best_match_date = None
        min_diff = float('inf')
        
        # Normalize JSON ex_date_obj to midnight for comparison
        ex_date_normalized = ex_date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
        
        for db_date in symbol_db_records.keys():
            diff = abs((db_date - ex_date_normalized).days)
            if diff <= day_tolerance and diff < min_diff:
                min_diff = diff
                best_match_date = db_date
        
        if best_match_date is None:
            missing_in_db.append({
                "symbol": symbol,
                "exDate": ex_date_iso,
                "jsonAmount": expected_amount,
                "jsonDividendStr": dividend_str,
                "dbFaceValue": actual_db_face_value
            })
        else:
            db_amount = symbol_db_records[best_match_date]
            amount_diff = abs(db_amount - expected_amount)
            
            # Use amount_tolerance to ignore minor discrepancies
            if amount_diff >= amount_tolerance:
                discrepancies.append({
                    "symbol": symbol,
                    "exDate": ex_date_iso,
                    "dbExDate": best_match_date.strftime("%Y-%m-%d"),
                    "dateDiffDays": min_diff,
                    "amountDiff": round(amount_diff, 4),
                    "dbAmount": db_amount,
                    "jsonAmount": expected_amount,
                    "jsonDividendStr": dividend_str,
                    "dbFaceValue": actual_db_face_value
                })
        
        processed_count += 1

    # 5. Summary and Output
    report = {
        "audit_timestamp": datetime.now().isoformat(),
        "summary": {
            "mode": "has_face_value" if has_face_value else "missing_face_value",
            "total_json_records": len(json_records),
            "processed_records": processed_count,
            "skipped_records": skipped_count,
            "audited_stocks_count": len(symbols_to_audit),
            "missing_in_db_count": len(missing_in_db),
            "discrepancies_count": len(discrepancies),
            "date_tolerance_days": day_tolerance,
            "amount_tolerance": amount_tolerance
        },
        "audited_stocks": symbols_with_face_value if has_face_value else symbols_without_face_value,
        "missing_in_db": missing_in_db,
        "discrepancies": discrepancies
    }

    logger.info(f"Audit complete. Results: {len(missing_in_db)} missing, {len(discrepancies)} discrepancies.")
    logger.info(f"Saving report to {output_file_path}...")
    
    with open(output_file_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Report saved to {output_file_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run dividend audit")
    parser.add_argument("--json", type=str, default="historical_dividends_audit.json", help="Source JSON file")
    parser.add_argument("--out", type=str, default="dividend_audit_report.json", help="Output report file")
    parser.add_argument("--tolerance", type=int, default=0, help="Day tolerance for exDate comparison")
    parser.add_argument("--amount-tolerance", type=float, default=0.0, help="Amount tolerance for discrepancy")
    parser.add_argument("--has-face-value", action="store_true", help="Audit only stocks that HAVE a faceValue")
    args = parser.parse_args()
    
    run_audit(args.json, args.out, args.tolerance, args.amount_tolerance, args.has_face_value)

# python3 src/psx/perform_dividend_audit.py --tolerance 7 --amount-tolerance 1.0

# python3 src/psx/perform_dividend_audit.py --tolerance 7 --amount-tolerance 1.0 --out dividend_audit_missing_facevalue_report.json