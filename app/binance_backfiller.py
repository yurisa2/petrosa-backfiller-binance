from pymongo import MongoClient
import os
from app import bin_data
import datetime
import time
import logging
import newrelic.agent


class BinanceBackfiller(object):

    def __init__(self, sender):
        self.client = MongoClient(
            os.getenv('MONGO_URI',
                      'mongodb://root:wUx3uQRBC8@localhost:27017/'))

        self.backfill_col = self.client.petrosa_crypto['backfill']
        self.sender = sender
        logging.warning('Starting backfiller')


    @newrelic.agent.background_task()
    def send_it_forward(self, df, period):
        send_list = df.to_dict('records')

        for row in send_list:
            prep_row = {}
            prep_row['k'] = row
            prep_row['k']['i'] = period
            prep_row['k']['origin'] = 'backfiller'
            prep_row['k']['petrosa_timestamp'] = datetime.datetime.now().isoformat()

            self.sender.send(prep_row)


    def continuous_run(self):
        while True:
            self.run()


    @newrelic.agent.background_task()
    def run(self):

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

        data = bin_data.get_data_bin(
            symbol=run_object['symbol'],
            startTime=int(start_ts*1000),
            endTime=int(end_ts*1000),
            interval=run_object['period']
            )

        self.send_it_forward(data, run_object['period'])

        # print('Finished backfiller for: ',
        #       run_object['symbol'],
        #       run_object['day'],
        #       run_object['period'])

        return data
