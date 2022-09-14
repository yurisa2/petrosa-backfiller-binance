import _thread
import websocket
import json
import time


class BinanceSocket(object):

    def __init__(self, sender, asset_list, period):
        self.sender = sender

        base_subscription = '@kline_'

        subscription = base_subscription + period

        self.asset_list = asset_list
        temp_table = []
        for result in self.asset_list:
            # print(result)
            temp_table.append(result['symbol'].lower() + subscription)

        self.asset_list = temp_table

        self.create_ws()
        pass

    def create_ws(self):
        while True:
            self.ws = websocket.WebSocketApp('wss://stream.binance.com:9443/ws/btcusdt@kline_15m',
                                             on_open=self.on_open,
                                             on_message=self.on_message,
                                             on_error=self.on_error,
                                             on_close=self.on_close
                                             )
            self.ws.run_forever()

    def on_message(self, ws, message):
        msg = json.loads(message)
        # print(msg)
        # if('k' in msg and msg['k']['s'] == 'FILUSDT'):
        #     print(msg)
        if('k' in msg and msg['k']['x'] == True):
            self.sender.to_send(msg)

    def on_error(self, ws, error):
        print(str(error))

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)

    def on_open(self, ws):
        _thread.start_new_thread(self.execute, ())

    def execute(self):
        unsub_msg = '''{
        "method": "UNSUBSCRIBE",
        "params":
        [
        "btcusdt@kline_15m"
        ],
        "id": 9999
        }'''
        self.ws.send(unsub_msg)

        counter = 0
        limits = 1
        for i in range(0, len(self.asset_list), limits):
            sub_list = self.asset_list[i:i+limits]
            # if('filusdt@kline_1m' in sub_list):
            #     print('SUBSCRIBING TO ASSETS', sub_list)
            subscription = {}
            subscription['method'] = "SUBSCRIBE"
            subscription['params'] = sub_list
            subscription['id'] = counter

            # print(subscription)
            # print(sub_list)

            counter += 1

            time.sleep(0.3)
            self.ws.send(json.dumps(subscription))

#
# if __name__ == "__main__":
#     # websocket.enableTrace(True)
#
#     ws.run_forever()
#     ws.run_forever()
