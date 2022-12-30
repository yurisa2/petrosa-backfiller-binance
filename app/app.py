from app import sender
from app import binance_backfiller
from datetime import datetime

start_datetime = datetime.utcnow()
sender = sender.PETROSASender('binance_backfill')

backfiller = binance_backfiller.BinanceBackfiller(sender)

while True:
    binance_backfiller.run()