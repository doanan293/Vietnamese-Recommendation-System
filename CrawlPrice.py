import pandas as pd
import logging
import requests
from datetime import datetime
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import re
import warnings
import pymongo
import numpy as np

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

# Define URL and headers for the API
API_VNDIRECT = 'https://finfo-api.vndirect.com.vn/v4/stock_prices/'
HEADERS = {
    'User-Agent': 'Your_User_Agent',
    'Authorization': 'Your_Authorization_Token'  # Add authorization token if required by the API
}

# Define utility functions
def convert_date(text, date_type='%Y-%m-%d'):
    return datetime.strptime(text, date_type)

def convert_text_dateformat(text, origin_type='%Y-%m-%d', new_type='%Y-%m-%d'):
    return convert_date(text, origin_type).strftime(new_type)

# Define DataLoader classes
class DataLoader():
    def __init__(self, symbols, start, end, minimal=True):
        self.symbols = symbols
        self.start = start
        self.end = end
        self.minimal = minimal

    def download(self):
        loader = DataLoaderVND(self.symbols, self.start, self.end)
        stock_data = loader.download()

        if self.minimal:
            data = stock_data.copy()  # Make a copy to avoid modifying the original DataFrame
            # Include 'date' column in the beginning
            data.reset_index(inplace=True)  # Reset index to convert 'date' from index to a regular column
            data = data[['date', 'high', 'low', 'open', 'close', 'avg', 'volume']]
            return data
        else:
            return stock_data

class DataLoadProto():
    def __init__(self, symbols, start, end):
        self.symbols = symbols
        self.start = convert_text_dateformat(start)
        self.end = convert_text_dateformat(end)

class DataLoaderVND(DataLoadProto):
    def __init__(self, symbols, start, end):
        super().__init__(symbols, start, end)

    def download(self):
        stock_datas = []
        if not isinstance(self.symbols, list):
            symbols = [self.symbols]
        else:
            symbols = self.symbols

        for symbol in symbols:
            stock_datas.append(self.download_one_new(symbol))

        data = pd.concat(stock_datas, axis=1)
        return data

    def download_one_new(self, symbol):
        start_date = self.start
        end_date = self.end
        query = 'code:' + symbol + '~date:gte:' + start_date + '~date:lte:' + end_date
        delta = convert_date(end_date) - convert_date(start_date)
        params = {
            "sort": "date",
            "size": delta.days + 1,
            "page": 1,
            "q": query
        }
        res = requests.get(API_VNDIRECT, params=params, headers=HEADERS)
        data = res.json()['data']
        data = pd.DataFrame(data)
        stock_data = data[['date', 'adClose', 'close', 'pctChange', 'average', 'nmVolume',
                           'nmValue', 'ptVolume', 'ptValue', 'open', 'high', 'low']].copy()
        stock_data.columns = ['date', 'adjust', 'close', 'change_perc', 'avg',
                              'volume_match', 'value_match', 'volume_reconcile', 'value_reconcile',
                              'open', 'high', 'low']

        stock_data = stock_data.set_index('date').apply(pd.to_numeric, errors='coerce')
        stock_data.index = list(map(convert_date, stock_data.index))
        stock_data.index.name = 'date'
        stock_data = stock_data.sort_index()
        stock_data.fillna(0, inplace=True)
        stock_data['volume'] = stock_data.volume_match + stock_data.volume_reconcile

        # Create multiple columns
        iterables = [stock_data.columns.tolist(), [symbol]]
        mulindex = pd.MultiIndex.from_product(iterables, names=['Attributes', 'Symbols'])
        stock_data.columns = mulindex

        logging.info('data {} from {} to {} have already cloned!'.format(symbol, self.start, self.end))

        return stock_data

vn30_companies = [
    "ACB"
]
#, "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB","TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
# Loop through each stock symbol in VN30 index
for stock in vn30_companies:
    loader = DataLoader(stock, '2007-01-01', '2030-04-02', minimal=True)
    data = loader.download()
    column_mapping = {
        'date': 'Date',
        'high': 'High',
        'low': 'Low',
        'open': 'Open',
        'close': 'Close',
        'avg': 'Avg',
        'volume': 'Volume'
    }
    data = data.rename(columns=column_mapping)
    # Extract column names from tuples
    data.columns = [col[0] for col in data.columns]
    # Specify the name for the collection
    data.to_csv(f"D:\\Study Program\\Project\\Price\\{stock}_price.csv", index=False)

