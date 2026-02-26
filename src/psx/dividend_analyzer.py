"""
Analysis script for PSX dividend announcements endpoint.
This script tests the AJAX endpoint to understand its structure before building the scraper.
"""

import requests
import json
from datetime import datetime
from pprint import pprint

# Endpoint configuration
ENDPOINT_URL = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx/chartact"
REFERER_URL = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx"


def analyze_endpoint():
    """
    Test the AJAX endpoint and print response structure.
    This helps understand the exact request format and response structure.
    """
    print("=" * 80)
    print("TESTING AJAX ENDPOINT")
    print("=" * 80)
    print(f"Endpoint: {ENDPOINT_URL}\n")

    # Standard headers for AJAX requests
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": REFERER_URL,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    # Test different payload formats
    # Based on jqGrid config: postData: {'par': par} with serializeGridData: JSON.stringify
    payloads_to_test = [
        # Empty par (get all records)
        {"par": ""},
        # With jqGrid pagination parameters
        {"par": "", "page": 1, "rows": 100},
        # With additional jqGrid standard params
        {"par": "", "page": 1, "rows": 100, "sidx": "", "sord": "asc"},
        # Just empty object
        {},
        # Try specific symbol
        {"par": "OGDC"},
    ]

    for i, payload in enumerate(payloads_to_test, 1):
        print(f"\n--- Test {i}: Payload = {payload} ---")
        try:
            response = requests.post(ENDPOINT_URL, json=payload, headers=headers, timeout=10)
            print(f"Status Code: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}\n")

            if response.status_code == 200:
                try:
                    data = response.json()
                    print("Response JSON structure:")
                    print(f"Keys: {list(data.keys())}")

                    # ASP.NET typically wraps response in 'd' key
                    if 'd' in data:
                        print("\nFound ASP.NET format with 'd' key")
                        inner_data = data['d']
                        print(f"Type of 'd': {type(inner_data)}")

                        if isinstance(inner_data, list) and len(inner_data) > 0:
                            print(f"Number of records: {len(inner_data)}")
                            print("\nFirst record structure:")
                            pprint(inner_data[0])
                            print("\nSample of first 3 records:")
                            for record in inner_data[:3]:
                                print(record)
                        elif isinstance(inner_data, dict):
                            print("Inner data is a dictionary:")
                            pprint(inner_data)
                    else:
                        print("\nDirect JSON response (no ASP.NET wrapper):")
                        pprint(data)

                    # Success - use this payload format
                    if response.status_code == 200 and data:
                        print(f"\n✓ Payload format {i} SUCCESSFUL")
                        return payload, data

                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")
                    print(f"Raw response text (first 500 chars): {response.text[:500]}")
            else:
                print(f"Error response: {response.text[:500]}")

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

    return None, None


def test_pagination(working_payload, rows_per_page=100):
    """
    Test pagination with different page numbers.
    """
    print("\n" + "=" * 80)
    print("TESTING PAGINATION")
    print("=" * 80)

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": REFERER_URL,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    # Test first 3 pages
    for page_num in range(1, 4):
        payload = working_payload.copy()
        payload['page'] = page_num
        payload['rows'] = rows_per_page

        print(f"\n--- Page {page_num} ---")
        try:
            response = requests.post(ENDPOINT_URL, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()

                # Handle ASP.NET format
                records = data.get('d', data)

                if isinstance(records, list):
                    print(f"Records on page {page_num}: {len(records)}")
                    if len(records) > 0:
                        print(f"First record: {records[0]}")
                    else:
                        print("No more records - reached end of data")
                        break
                elif isinstance(records, dict):
                    # jqGrid format with total, page, records, rows
                    if 'rows' in records:
                        print(f"Total records: {records.get('records', 'N/A')}")
                        print(f"Total pages: {records.get('total', 'N/A')}")
                        print(f"Current page: {records.get('page', 'N/A')}")
                        print(f"Records on this page: {len(records['rows'])}")
                        if records['rows']:
                            print(f"First record: {records['rows'][0]}")
            else:
                print(f"Error: Status {response.status_code}")

        except Exception as e:
            print(f"Request failed: {e}")


def parse_response(response_data):
    """
    Parse the JSON response and extract field information.
    """
    print("\n" + "=" * 80)
    print("PARSING RESPONSE STRUCTURE")
    print("=" * 80)

    # Handle ASP.NET format
    records = response_data.get('d', response_data)

    if isinstance(records, dict) and 'rows' in records:
        # jqGrid format
        records = records['rows']

    if not isinstance(records, list) or len(records) == 0:
        print("No records to parse")
        return

    # Analyze first record
    first_record = records[0]
    print("\nField Analysis:")
    print("-" * 80)

    for key, value in first_record.items():
        value_type = type(value).__name__
        sample = str(value)[:50] if value else "None"
        print(f"{key:15s} | {value_type:10s} | {sample}")

    # Collect unique field names from first 10 records
    all_fields = set()
    for record in records[:10]:
        all_fields.update(record.keys())

    print(f"\nAll unique fields found: {sorted(all_fields)}")

    # Check for expected fields
    expected_fields = ['CODE', 'Name', 'Dividend', 'Bonus', 'Right', 'xDate']
    print(f"\nExpected fields: {expected_fields}")

    for field in expected_fields:
        # Case-insensitive check
        found = any(f.lower() == field.lower() for f in all_fields)
        status = "✓" if found else "✗"
        print(f"{status} {field}")


def test_date_parsing(sample_dates):
    """
    Test date parsing with various formats.
    """
    print("\n" + "=" * 80)
    print("TESTING DATE PARSING")
    print("=" * 80)

    date_formats = [
        "%d %b %Y",      # 15 Jan 2025
        "%d-%b-%Y",      # 15-Jan-2025
        "%d/%m/%Y",      # 15/01/2025
        "%Y-%m-%d",      # 2025-01-15
        "%B %d, %Y",     # January 15, 2025
        "%d %B %Y",      # 15 January 2025
    ]

    for date_str in sample_dates:
        print(f"\nTesting: '{date_str}'")
        parsed = False

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                print(f"  ✓ Matched format: {fmt} → {dt}")
                parsed = True
                break
            except ValueError:
                continue

        if not parsed:
            print(f"  ✗ Could not parse with any known format")


def main():
    """
    Main analysis function.
    """
    print("\n" + "=" * 80)
    print("PSX DIVIDEND ANNOUNCEMENTS - ENDPOINT ANALYZER")
    print("=" * 80)
    print(f"Analysis started at: {datetime.now()}\n")

    # Step 1: Test endpoint and find working payload
    print("\nStep 1: Testing endpoint...")
    working_payload, response_data = analyze_endpoint()

    if not working_payload:
        print("\n✗ Could not find a working payload format")
        print("The endpoint may have changed or requires authentication")
        return

    print(f"\n✓ Found working payload: {working_payload}")

    # Step 2: Parse response structure
    if response_data:
        parse_response(response_data)

    # Step 3: Test pagination
    print("\nStep 2: Testing pagination...")
    test_pagination(working_payload)

    # Step 4: Test date parsing with sample dates
    print("\nStep 3: Testing date parsing...")
    sample_dates = [
        "20 Apr 2026",
        "17 Apr 2026",
        "09 Mar 2026",
        "19-Mar-2026",
        "2026-03-19"
    ]
    test_date_parsing(sample_dates)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print("=" * 80)
    print("\nNext steps:")
    print("1. Review the output above to understand the endpoint structure")
    print("2. Note the working payload format for the scraper")
    print("3. Identify the correct field names (case-sensitive)")
    print("4. Determine the date format for parsing")
    print("5. Proceed to build dividend_scraper.py")
    print("=" * 80)


if __name__ == "__main__":
    main()
