import requests
import pandas as pd
import datetime


def get_data_bin(symbol,
                 startTime: int,
                 endTime: int,
                 interval='1h'):

    url = 'https://fapi.binance.com/fapi/v1/klines'

    result = requests.get(
        url, {'symbol': symbol,
              'interval': interval,
              'startTime': startTime,
              'endTime': endTime
              }
              )

    df = pd.DataFrame(columns=['datetime',
                               'open',
                               'high',
                               'low',
                               'close',
                               'close_time',
                               'closed_candle',
                               'qty',
                               'vol',
                               'insert_time',
                               'origin',

                               ])

    lticker, ldatetime, lopen, lhigh, llow, lclose, lclose_time, lclosed_candle, lqty, lvol, linsert_time, lorigin = [
        ], [], [], [], [], [], [], [], [], [], [], []

    for candle in result.json():
        lticker.append(symbol)
        ldatetime.append(datetime.datetime.fromtimestamp(candle[0]/1000.0))
        lopen.append(float(candle[1]))
        lhigh.append(float(candle[2]))
        llow.append(float(candle[3]))
        lclose.append(float(candle[4]))
        lvol.append(float(candle[5]))
        lclose_time.append(datetime.datetime.fromtimestamp(candle[6]/1000.0))
        lqty.append(float(candle[8]))
        linsert_time.append(datetime.datetime.now())
        lclosed_candle.append(True)
        lorigin.append('API')

    df['ticker'] = lticker
    df['datetime'] = ldatetime
    df['open'] = lopen
    df['high'] = lhigh
    df['low'] = llow
    df['close'] = lclose
    df['close_time'] = lclose_time
    df['closed_candle'] = lclosed_candle
    df['qty'] = lqty
    df['vol'] = lvol
    df['insert_time'] = linsert_time

    # df = df.set_index('datetime')

    return df

day = "2021-10-20"
start_ts = datetime.datetime.strptime(day, '%Y-%m-%d').timestamp()
end_ts = start_ts + 86399



get_data_bin('ETHUSDT', int(start_ts*1000), int(end_ts*1000))
