"""
PSX Dividend Announcements Scraper.
Scrapes dividend, bonus, and rights announcements from scstrade.com.
"""

import requests
import pandas as pd
import threading
import logging
from datetime import datetime
from typing import Optional, List, Dict
import time

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DividendScraper:
    """
    Scraper for PSX dividend, bonus, and rights announcements.

    Based on analysis of scstrade.com xDates page which uses jqGrid with
    loadonce=true (all data loaded in single request).
    """

    def __init__(self):
        """Initialize the scraper with endpoint configuration."""
        self.__endpoint = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx/chartact"
        self.__referer = "https://www.scstrade.com/MarketStatistics/MS_xDates.aspx"
        self.__local = threading.local()
        logger.info("DividendScraper initialized")

    @property
    def session(self):
        """
        Thread-safe session property.
        Creates a new session for each thread following pattern from web.py.
        """
        if not hasattr(self.__local, "session"):
            self.__local.session = requests.Session()
        return self.__local.session

    def fetch_all_announcements(self) -> pd.DataFrame:
        """
        Fetch all dividend announcements in a single request.

        The endpoint returns all records at once (jqGrid loadonce: true).
        No pagination needed.

        Returns:
            pd.DataFrame: DataFrame with all announcements
        """
        logger.info("Fetching all dividend announcements...")

        try:
            # Make request
            response_data = self._make_request()

            if not response_data:
                logger.warning("No data received from endpoint")
                return pd.DataFrame()

            # Parse records
            records = self._parse_records(response_data)

            if not records:
                logger.warning("No records parsed from response")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(records)
            logger.info(f"Successfully fetched {len(df)} announcements")

            return df

        except Exception as e:
            logger.error(f"Error fetching announcements: {str(e)}", exc_info=True)
            raise

    def _make_request(self) -> Optional[Dict]:
        """
        Make POST request to the AJAX endpoint.

        Returns:
            Optional[Dict]: JSON response or None if failed
        """
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": self.__referer,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest"
        }

        # Payload: empty 'par' parameter gets all records
        payload = {"par": ""}

        try:
            logger.debug(f"Making POST request to {self.__endpoint}")
            response = self.session.post(
                self.__endpoint,
                json=payload,
                headers=headers,
                timeout=30
            )

            response.raise_for_status()

            data = response.json()
            logger.debug(f"Received response with status {response.status_code}")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            return None
        except ValueError as e:
            logger.error(f"JSON decode failed: {str(e)}")
            return None

    def _parse_records(self, response_data: Dict) -> List[Dict]:
        """
        Parse records from ASP.NET JSON response.

        Args:
            response_data: Response JSON (ASP.NET format with 'd' key)

        Returns:
            List[Dict]: List of parsed announcement records
        """
        # Handle ASP.NET format: {d: [...]}
        records = response_data.get('d', response_data)

        if not isinstance(records, list):
            logger.error(f"Expected list of records, got {type(records)}")
            return []

        logger.info(f"Parsing {len(records)} records...")

        parsed_records = []
        for i, record in enumerate(records):
            try:
                parsed = self.parse_announcement(record)
                if parsed:
                    parsed_records.append(parsed)
            except Exception as e:
                logger.warning(f"Failed to parse record {i}: {str(e)}")
                continue

        logger.info(f"Successfully parsed {len(parsed_records)} records")
        return parsed_records

    def parse_announcement(self, record: Dict) -> Optional[Dict]:
        """
        Parse a single announcement record.

        Field mapping:
        - company_code → symbol
        - company_name → name
        - bm_dividend → dividend (e.g., "20%")
        - bm_bonus → bonus
        - bm_right_per → right
        - bm_bc_exp → x_date (ex-date)
        - sector_name → sector

        Args:
            record: Raw record from API

        Returns:
            Optional[Dict]: Parsed record or None if invalid
        """
        try:
            # Extract fields
            symbol = record.get('company_code', '').strip()
            name = record.get('company_name', '').strip()
            dividend_str = record.get('bm_dividend', '').strip()
            bonus_str = record.get('bm_bonus', '').strip()
            right_str = record.get('bm_right_per', '').strip()
            x_date_str = record.get('bm_bc_exp', '').strip()
            sector = record.get('sector_name', '').strip()

            # Skip if no symbol or no ex-date
            if not symbol or not x_date_str:
                return None

            # Parse date
            x_date = self.parse_date(x_date_str)
            if not x_date:
                logger.warning(f"Could not parse date '{x_date_str}' for {symbol}")
                return None

            # Parse dividend percentage
            dividend = self._parse_percentage(dividend_str)

            # Determine announcement types
            announcement_types = []
            if dividend_str:
                announcement_types.append("dividend")
            if bonus_str:
                announcement_types.append("bonus")
            if right_str:
                announcement_types.append("right")

            # Skip if no announcements
            if not announcement_types:
                return None

            return {
                "symbol": symbol,
                "name": name,
                "dividend": dividend,
                "bonus": bonus_str,
                "right": right_str,
                "x_date": x_date,
                "sector": sector,
                "announcement_type": announcement_types,
                "scraped_at": datetime.now()
            }

        except Exception as e:
            logger.warning(f"Error parsing record: {str(e)}")
            return None

    def parse_date(self, date_string: str) -> Optional[datetime]:
        """
        Parse date string to datetime object.

        Expected format: "20 Apr 2026" (%d %b %Y)
        Also tries alternative formats as fallback.

        Args:
            date_string: Date string to parse

        Returns:
            Optional[datetime]: Parsed datetime or None
        """
        if not date_string:
            return None

        # Try multiple formats
        formats = [
            "%d %b %Y",      # 20 Apr 2026
            "%d-%b-%Y",      # 20-Apr-2026
            "%d/%m/%Y",      # 20/04/2026
            "%Y-%m-%d",      # 2026-04-20
            "%B %d, %Y",     # April 20, 2026
            "%d %B %Y",      # 20 April 2026
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_string.strip(), fmt)
            except ValueError:
                continue

        logger.warning(f"Could not parse date: '{date_string}'")
        return None

    def _parse_percentage(self, percent_str: str) -> float:
        """
        Parse percentage string to float.

        Args:
            percent_str: Percentage string (e.g., "20%", "2160%")

        Returns:
            float: Percentage value or 0.0 if invalid
        """
        if not percent_str:
            return 0.0

        try:
            # Remove % sign and convert to float
            clean_str = percent_str.replace('%', '').strip()
            return float(clean_str)
        except ValueError:
            logger.warning(f"Could not parse percentage: '{percent_str}'")
            return 0.0


def main():
    """
    Test the scraper standalone.
    """
    print("=" * 80)
    print("PSX DIVIDEND SCRAPER - STANDALONE TEST")
    print("=" * 80)

    scraper = DividendScraper()

    print("\nFetching announcements...")
    df = scraper.fetch_all_announcements()

    if df.empty:
        print("No announcements found")
        return

    print(f"\n✓ Fetched {len(df)} announcements")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nFirst 5 records:")
    print(df.head())

    # Statistics
    print(f"\n" + "=" * 80)
    print("STATISTICS")
    print("=" * 80)
    print(f"Total announcements: {len(df)}")
    print(f"\nBy type:")
    print(f"  Dividend: {df['announcement_type'].apply(lambda x: 'dividend' in x).sum()}")
    print(f"  Bonus: {df['announcement_type'].apply(lambda x: 'bonus' in x).sum()}")
    print(f"  Rights: {df['announcement_type'].apply(lambda x: 'right' in x).sum()}")

    print(f"\nDividend range: {df['dividend'].min():.2f}% - {df['dividend'].max():.2f}%")
    print(f"Date range: {df['x_date'].min()} to {df['x_date'].max()}")

    print(f"\n" + "=" * 80)


if __name__ == "__main__":
    main()
