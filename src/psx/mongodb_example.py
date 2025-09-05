"""
Example script demonstrating how to fetch PSX data and store it in MongoDB.
"""

from psx import stocks
from psx.data_store import save_to_mongodb
import datetime

def main():
    # Define the stock symbol and date range
    symbol = "MEBL"
    start_date = datetime.date(2025, 8, 1)
    end_date = datetime.date.today()
    
    print(f"Fetching data for {symbol} from {start_date} to {end_date}")
    
    # Fetch stock data
    data = stocks(symbol, start=start_date, end=end_date)
    
    # Display basic information about the data
    print(f"Shape: {data.shape}")
    print(f"Columns: {data.columns.tolist()}")
    print(f"First 5 rows:")
    print(data.head())
    
    # Save data to MongoDB
    # Modify the connection string as needed for your MongoDB setup
    connection_string = "mongodb://192.168.0.131:27017/"
    db_name = "finhisaab"
    collection_name = "stockpricehistories"
    
    print(f"\nSaving data to MongoDB ({db_name}.{collection_name})...")
    
    success, message = save_to_mongodb(
        df=data,
        symbol=symbol,
        connection_string=connection_string,
        db_name=db_name,
        collection_name=collection_name
    )
    
    print(f"MongoDB Save Result: {'Success' if success else 'Failed'}")
    print(f"Message: {message}")
    
    if success:
        print("\nData has been successfully stored in MongoDB")
#         print("""
# {
#     "_id": ObjectId("..."),
#     "symbol": "MEBL", 
#     "date": ISODate("2025-08-01T00:00:00Z"),
#     "open": 360.45,
#     "high": 364.05,
#     "low": 358.02,
#     "close": 362.99,
#     "volume": 499099.0,
#     "created_at": ISODate("2025-09-04T18:39:44Z"),
#     "updated_at": ISODate("2025-09-04T18:39:44Z")
# }
#         """)
        
#         print("\nTo query this data in MongoDB, you can use:")
#         print("""
# # Using the MongoDB shell
# db.stockpricehistories.find({ "symbol": "MEBL" })

# # Get data for a specific date
# db.stockpricehistories.find({ "symbol": "MEBL", "date": ISODate("2025-08-01T00:00:00Z") })

# # Get data for a date range
# db.stockpricehistories.find({ 
#     "symbol": "MEBL", 
#     "date": { 
#         "$gte": ISODate("2025-08-01T00:00:00Z"),
#         "$lte": ISODate("2025-08-15T00:00:00Z")
#     } 
# })
#         """)

if __name__ == "__main__":
    main()
