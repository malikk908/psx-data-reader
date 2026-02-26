"""
Cron script for scraping PSX dividend announcements and storing in MongoDB.
This script runs as a standalone job, separate from the price data scraper.
"""

from psx.dividend_scraper import DividendScraper
from psx.dividend_store import save_announcements_to_mongodb, get_collection_stats
import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


def test_mongo_connectivity(connection_string: str, db_name: str) -> bool:
    """
    Perform a quick connectivity test to MongoDB.

    Attempts to connect and run a simple ping command. Returns True if
    successful, False otherwise.

    Args:
        connection_string: MongoDB connection string
        db_name: Database name

    Returns:
        bool: True if connectivity test passed, False otherwise
    """
    try:
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        # Run a simple admin ping to verify connectivity
        client.admin.command('ping')
        # Optionally touch the target DB to ensure we can access it
        _ = client[db_name].name
        print(f"MongoDB connectivity OK for {connection_string} (db: {db_name})")
        return True
    except Exception as e:
        print(f"MongoDB connectivity check FAILED: {e}")
        return False
    finally:
        try:
            if 'client' in locals():
                client.close()
        except Exception:
            pass


def main():
    """
    Main function for the dividend announcements cron job.
    """
    print("=" * 80)
    print("PSX DIVIDEND ANNOUNCEMENTS - CRON JOB")
    print("=" * 80)
    print(f"Started at: {__import__('datetime').datetime.now()}\n")

    # MongoDB connection settings via environment variables
    # Provide sensible defaults for local development
    connection_string = os.getenv("FINHISAAB_MONGO_URI", "mongodb://192.168.0.131:27017/")
    db_name = os.getenv("FINHISAAB_DB_NAME", "finhisaab")
    collection_name = os.getenv("FINHISAAB_DIVIDEND_COLLECTION", "dividend_announcements")

    # Early connectivity test to fail fast if DB is unreachable
    if not test_mongo_connectivity(connection_string, db_name):
        print("Exiting due to MongoDB connectivity failure.")
        return

    # Initialize scraper
    print("\n" + "=" * 80)
    print("STEP 1: SCRAPING ANNOUNCEMENTS")
    print("=" * 80)
    print("Initializing dividend scraper...")

    try:
        scraper = DividendScraper()
    except Exception as e:
        print(f"Failed to initialize scraper: {e}")
        return

    # Fetch all announcements
    print("Fetching dividend announcements from scstrade.com...")
    try:
        announcements = scraper.fetch_all_announcements()

        if announcements.empty:
            print("No announcements found. Exiting.")
            return

        print(f"✓ Successfully fetched {len(announcements)} announcements")

        # Print summary
        dividend_count = announcements['announcement_type'].apply(
            lambda x: 'dividend' in x).sum()
        bonus_count = announcements['announcement_type'].apply(
            lambda x: 'bonus' in x).sum()
        rights_count = announcements['announcement_type'].apply(
            lambda x: 'right' in x).sum()

        print(f"\nAnnouncement breakdown:")
        print(f"  Dividend: {dividend_count}")
        print(f"  Bonus: {bonus_count}")
        print(f"  Rights: {rights_count}")

        print(f"\nDate range: {announcements['x_date'].min()} to {announcements['x_date'].max()}")

    except Exception as e:
        print(f"Scraping failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # Save to MongoDB
    print("\n" + "=" * 80)
    print("STEP 2: SAVING TO MONGODB")
    print("=" * 80)
    print(f"Database: {db_name}")
    print(f"Collection: {collection_name}")
    print(f"Saving {len(announcements)} announcements...")

    try:
        success, message = save_announcements_to_mongodb(
            df=announcements,
            connection_string=connection_string,
            db_name=db_name,
            collection_name=collection_name
        )

        print(f"\nResult: {'✓ Success' if success else '✗ Failed'}")
        print(f"Message: {message}")

        if not success:
            print("\nMongoDB save operation failed. Check logs above.")
            return

    except Exception as e:
        print(f"MongoDB save failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return

    # Get and display collection statistics
    print("\n" + "=" * 80)
    print("STEP 3: COLLECTION STATISTICS")
    print("=" * 80)

    try:
        stats = get_collection_stats(
            connection_string=connection_string,
            db_name=db_name,
            collection_name=collection_name
        )

        print(f"\nTotal announcements in database: {stats.get('total_announcements', 'N/A')}")
        print(f"Date range: {stats.get('earliest_x_date', 'N/A')} to {stats.get('latest_x_date', 'N/A')}")
        print(f"\nBy type:")
        print(f"  Dividend: {stats.get('dividend_announcements', 'N/A')}")
        print(f"  Bonus: {stats.get('bonus_announcements', 'N/A')}")
        print(f"  Rights: {stats.get('rights_announcements', 'N/A')}")

    except Exception as e:
        print(f"Failed to get collection stats: {e}")

    # Summary
    print("\n" + "=" * 80)
    print("CRON JOB COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"Completed at: {__import__('datetime').datetime.now()}")
    print("=" * 80)


if __name__ == "__main__":
    main()
