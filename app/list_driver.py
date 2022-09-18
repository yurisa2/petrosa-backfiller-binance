import requests
import datetime
from pymongo import MongoClient

client = MongoClient('mongodb://root:wUx3uQRBC8@localhost:27017/')

asset_list_raw = requests.get(
    'https://fapi.binance.com/fapi/v1/ticker/price').json()


periods = ['5m', '15m', '30m', '1h']


start_date = datetime.date.today()

number_of_days = 365
date_list = []
date_list.append('2022-07-15')


item_list = []
for symbol in asset_list_raw:
    for day in date_list:
        for period in periods:
            item = {}
            item['symbol'] = symbol['symbol']
            item['day'] = day
            item['period'] = period
            item['state'] = 0
            item_list.append(item)

len(item_list)


db = client.petrosa_crypto
collection = db['backfill']

collection.insert_many(item_list)
