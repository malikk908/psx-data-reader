from psx import stocks, tickers
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from psx.data_store import save_to_mongodb

import datetime


# tickers = tickers()

start = datetime.date(2020, 8, 1)
end = datetime.date(2021, 2, 1)

data = stocks(["OGDC", "MEBL"], start=start, end=end)

# Shape of data (rows, columns)
print(f"Shape: {data.shape}")

# Column names
print(f"Columns: {data.columns.tolist()}")

# Data types
print(data.dtypes)

# First 5 rows
print(data.head())

# Last 5 rows
print(data.tail())

# Save data to MongoDB
# Uncomment and configure the connection string as needed
# success, message = save_to_mongodb(
#     df=data,
#     symbol="MEBL",
#     connection_string="mongodb://localhost:27017/",
#     db_name="psx_stocks",
#     collection_name="stock_data"
# )
# print(f"MongoDB Save Result: {success}")
# print(f"Message: {message}")

# Statistical summary
# print(data.describe())

# fig = go.Figure()

# fig = make_subplots(rows=1, cols=1)

# fig = make_subplots(rows=2,
#                     cols=1,
#                     shared_xaxes=True,
#                     vertical_spacing=0.1,
#                     subplot_titles=('EFERT', 'Volume'),
#                     row_width=[0.3, 0.7])



# fig.append_trace(
#     go.Candlestick(
#         x=data.index,
#         open=data.Open,
#         high=data.High,
#         low=data.Low,
#         close=data.Close,  
#     ), row=1, col=1
# )
# fig.add_trace(
#     go.Bar(x=data.index,
#            y=data.Volume,
#            marker_color="green",
#            showlegend=False),
#     row=2,
#     col=1
# )
# # volume_bar = go.Figure()

# fig.update_layout(title="EFERT Stocks",
#                   yaxis_title="Price (PKR)",
#                   width=1400,
#                   height=700)

# fig.update(layout_xaxis_rangeslider_visible=False)
# fig.show()

