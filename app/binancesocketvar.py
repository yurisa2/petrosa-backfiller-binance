import _thread
import websocket
import json
import time


class BinanceSocket(object):

    def __init__(self, sender, asset_list):
        self.sender = sender

        base_subscription = '@miniTicker'

        subscription = base_subscription

        self.asset_list = asset_list
        temp_table = []
        for result in self.asset_list:
            temp_table.append(result['symbol'].lower() + subscription)

        self.asset_list = temp_table

        self.create_ws()
        pass

    def create_ws(self):
        while True:
            self.ws = websocket.WebSocketApp('wss://stream.binance.com:9443/ws/btcusdt@miniTicker',
                                             on_open=self.on_open,
                                             on_message=self.on_message,
                                             on_error=self.on_error,
                                             on_close=self.on_close
                                             )
            self.ws.run_forever()

    def on_message(self, ws, message):
        msg = json.loads(message)

        try:
            if('s' in msg):
                ticker = msg['s']
                close = float(msg['c'])
                open = float(msg['o'])

                var_price = {}
                var_price['ticker'] = ticker
                var_price['var'] = ((close / open) - 1)

                if(len(var_price) > 1):
                    self.sender.to_send(var_price)
                    # print(var_price)
        except Exception as e:
            print(e)

    def on_error(self, ws, error):
        print(str(error))

    def on_close(self, ws, close_status_code, close_msg):
        print("### closed ###", close_status_code, close_msg)

    def on_open(self, ws):
        print("### opened ###")
        _thread.start_new_thread(self.execute, ())

    def execute(self):
        unsub_msg = '''{
        "method": "UNSUBSCRIBE",
        "params":
        [
        "btcusdt@miniTicker"
        ],
        "id": 9999
        }'''
        self.ws.send(unsub_msg)

        counter = 0
        limits = 1
        for i in range(0, len(self.asset_list), limits):
            sub_list = self.asset_list[i:i+limits]

            subscription = {}
            subscription['method'] = "SUBSCRIBE"
            subscription['params'] = sub_list
            subscription['id'] = counter

            # print(subscription)
            # print(subscription)

            counter += 1

            time.sleep(0.3)
            self.ws.send(json.dumps(subscription))
