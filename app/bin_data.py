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

    df = pd.DataFrame()

    lticker, ldatetime, lopen, lhigh, llow, lclose, lclose_time, lclosed_candle, lqty, lvol, = [
        ], [], [], [], [], [], [], [], [], []

    for candle in result.json():
        lticker.append(symbol)
        ldatetime.append(candle[0])
        lopen.append(float(candle[1]))
        lhigh.append(float(candle[2]))
        llow.append(float(candle[3]))
        lclose.append(float(candle[4]))
        lvol.append(float(candle[5]))
        lclose_time.append(candle[6])
        lqty.append(float(candle[8]))
        lclosed_candle.append(True)

    df['s'] = lticker
    df['t'] = ldatetime
    df['o'] = lopen
    df['h'] = lhigh
    df['l'] = llow
    df['c'] = lclose
    df['T'] = lclose_time
    df['x'] = lclosed_candle
    df['n'] = lqty
    df['n'] = lvol

    # df = df.set_index('datetime')

    return df
