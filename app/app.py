from app import sender
from app import binance_backfiller
from datetime import datetime
import queue
from app import receiver 



start_datetime = datetime.utcnow()
sender = sender.PETROSASender('binance_backfill')
msg_queue = queue.Queue()

rec = receiver.PETROSAReceiver("binance_intraday_backfilling", msg_queue)

backfiller = binance_backfiller.BinanceBackfiller(sender, msg_queue)

while True:
    backfiller.run_from_db()
    backfiller.run_from_intraday()
