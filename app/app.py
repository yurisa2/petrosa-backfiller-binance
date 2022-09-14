import os
from app import sender
import threading
from datetime import datetime
import requests


from flask import Flask

app = Flask(__name__)

start_datetime = datetime.utcnow()

asset_list_raw = requests.get(
    'https://fapi.binance.com/fapi/v1/ticker/price').json()


asset_list_full = []
for item in asset_list_raw:
    if(item['symbol'][-4:] == 'USDT' or item['symbol'][-4:] == 'BUSD'):
        # print(item)
        asset_list_full.append(item)


@app.route("/")
def default():

    return "ok", 200


@app.route("/status")
def queues():
    queues = {}

    queues['start_datetime'] = start_datetime

    return queues, 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
