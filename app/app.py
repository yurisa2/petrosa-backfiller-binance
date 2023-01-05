import queue
from datetime import datetime

from app import binance_backfiller, receiver, sender

start_datetime = datetime.utcnow()
sender = sender.PETROSASender('binance_backfill')
msg_queue = queue.Queue()

rec = receiver.PETROSAReceiver("binance_intraday_backfilling", msg_queue)

backfiller = binance_backfiller.BinanceBackfiller(sender, msg_queue)

while True:
    backfiller.run_from_db()
    backfiller.run_from_intraday()
