import _thread
import websocket
import json
import time
import bin_data
from pymongo import MongoClient
import os

client = MongoClient(os.getenv('MONGO_URI', 'mongodb://root:wUx3uQRBC8@localhost:27017/'))


class BinanceBackfiller(object):

    def __init__(self, sender, asset_list, period):
        pass


    def run(self):
