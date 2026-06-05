from concurrent.futures import ThreadPoolExecutor, as_completed, _base
from dateutil.relativedelta import relativedelta
from pandas import DataFrame as container
from bs4 import BeautifulSoup as parser
from collections import defaultdict
from datetime import datetime, date
from typing import Union
from tqdm import tqdm

import threading
import pandas as pd
import numpy as np
from curl_cffi import requests
import time
from pdb import set_trace


class DataReader:

    headers = ['TIME', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']

    def __init__(self):
        self.__history = "https://dps.psx.com.pk/historical"
        self.__symbols = "https://dps.psx.com.pk/symbols"
        self.__local = threading.local()

    @property
    def session(self):
        if not hasattr(self.__local, "session"):
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Referer": "https://dps.psx.com.pk/"
            }
            self.__local.session = requests.Session(impersonate="chrome", headers=headers)
        return self.__local.session

    def tickers(self):
        return pd.read_json(self.__symbols)

    def get_psx_data(self, symbol: str, dates: list) -> container:
        data = futures = []
        
        with tqdm(total=len(dates), desc="Downloading {}'s Data".format(symbol)) as progressbar:

            with ThreadPoolExecutor(max_workers=6) as executor:
                for date in dates:
                    futures.append(executor.submit(self.download, symbol=symbol, date=date))

                for future in as_completed(futures):
                    data.append(future.result())
                    progressbar.update(1)
            
            data = [instance for instance in data if isinstance(instance, container)]
        
        return self.preprocess(data)
    
    def stocks(self, tickers: Union[str, list], start: date, end: date) -> container:
        tickers = [tickers] if isinstance(tickers, str) else tickers
        dates = self.daterange(start, end)

        data = [self.get_psx_data(ticker, dates)[start: end] for ticker in tickers]

        if len(data) == 1:
            return data[0]

        return pd.concat(data, keys=tickers, names=["Ticker", "Date"])


    def download(self, symbol: str, date: date):
        session = self.session
        post = {"month": date.month, "year": date.year, "symbol": symbol}
        response = session.post(self.__history, data=post)
        data = parser(response.text, features="html.parser")
        data = self.toframe(data)
        return data

    def toframe(self, data):
        stocks = defaultdict(list)
        rows = data.select("tr")

        for row in rows:
            cols = [col.getText() for col in row.select("td")]
        
            for key, value in zip(self.headers, cols):
                if key == "TIME":
                    value = datetime.strptime(value, "%b %d, %Y")
                stocks[key].append(value)

        return pd.DataFrame(stocks, columns=self.headers).set_index("TIME")

    def daterange(self, start: date, end: date) -> list:
        period = end - start
        number_of_months = period.days // 30
        current_date = datetime(start.year, start.month, 1)
        dates = [current_date]

        for month in range(number_of_months):
            prev_date = dates[-1]
            dates.append(prev_date + relativedelta(months=1))

        dates = dates if len(dates) else [start]
        return dates

    def preprocess(self, data: list) -> pd.DataFrame:
        # concatenate each frame to a single dataframe
        data = pd.concat(data)
        # sort the data by date
        data = data.sort_index()
        # change indexes from all uppercase to title
        data = data.rename(columns=str.title)
        # change index label Title to Date
        data.index.name = "Date"
        # remove non-numeric characters from volume column 
        data.Volume = data.Volume.str.replace(",", "")
        # coerce each column type to float
        for column in data.columns:
            data[column] = data[column].str.replace(",", "").astype(np.float64)
        return data


data_reader = DataReader()

if __name__ == "__main__":
    data = data_reader.stocks("OGDC", date(2018, 12, 10), date(2019, 12, 10))