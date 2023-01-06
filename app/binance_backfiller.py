import datetime
import logging
import os
import time

import newrelic.agent
from pymongo import MongoClient

from app import bin_data


class BinanceBackfiller(object):

    def __init__(self, sender, msg_queue):
        self.client = MongoClient(
            os.getenv('MONGO_URI',
                      'mongodb://root:QnjfRW7nl6@localhost:27017'))

        self.backfill_col = self.client.petrosa_crypto['backfill']
        self.sender = sender
        self.msg_queue = msg_queue
        logging.warning('Starting backfiller')


    @newrelic.agent.background_task()
    def send_it_forward(self, df, period, origin):
        send_list = df.to_dict('records')

        for row in send_list:
            prep_row = {}
            prep_row['k'] = row
            prep_row['k']['i'] = period
            prep_row['k']['origin'] = origin
            prep_row['k']['petrosa_timestamp'] = datetime.datetime.utcnow().isoformat()

            self.sender.send(prep_row)

    @newrelic.agent.background_task()
    def manage_data(self, 
                    symbol: str, 
                    start_ts: int, 
                    end_ts: int, 
                    interval: str, 
                    origin: str):
        data = bin_data.get_data_bin(
            symbol=symbol,
            startTime=int(round(start_ts*1000)),
            endTime=int(round(end_ts*1000)),
            interval=interval
            )

        if data is not None:
            self.send_it_forward(data, interval, origin)
        return True
    
    @newrelic.agent.background_task()
    def run_from_intraday(self):
        for _ in (range(10)):
            try:
                msg = self.msg_queue.get_nowait()
                self.manage_data(msg['ticker'], 
                                int(msg['min']), 
                                int(msg['max']), 
                                msg['period'],
                                'backfiller_intraday')
            except Exception as e:
                pass
            


    @newrelic.agent.background_task()
    def run_from_db(self):
        run_object = self.backfill_col.find({"state": 0}).sort("checking_times", 1).limit(1)

        run_object = list(run_object)
        if(len(run_object) > 0):
            run_object = run_object[0]
        else:
            run_object = None

        if not run_object:
            logging.warning('Nothing to backfill, KUDOS!')
            time.sleep(60)
            return True

        self.backfill_col.update_one(run_object, {'$set': {"state": 1}})

        day = run_object['day']
        start_ts = datetime.datetime.strptime(day, '%Y-%m-%d').timestamp()
        end_ts = start_ts + 86399
        
        self.manage_data(run_object['symbol'], 
                        int(start_ts), 
                        int(end_ts), 
                        run_object['period'],
                        'backfiller')

        return True
