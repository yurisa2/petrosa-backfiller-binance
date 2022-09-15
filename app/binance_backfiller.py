from pymongo import MongoClient
import os
from app import bin_data
import datetime


class BinanceBackfiller(object):

    def __init__(self, sender):
        self.client = MongoClient(
            os.getenv('MONGO_URI',
                      'mongodb://root:wUx3uQRBC8@localhost:27017/'))

        self.backfill_col = self.client.petrosa_crypto['backfill']
        self.sender = sender

    def send_it_forward(self, df, period):
        send_list = df.to_dict('records')

        for row in send_list:
            prep_row = {}
            prep_row['k'] = row
            prep_row['k']['i'] = period

            self.sender.send(prep_row)

    def continuous_run(self):
        while True:
            try:
                self.run()
            except Exception as e:
                print(e)

    def run(self):
        print('Starting backfiller', datetime.datetime.now())

        run_object = self.backfill_col.find_one({"state": 0})
        self.backfill_col.update_one(run_object, {'$set': {"state": 1}})

        day = run_object['day']
        start_ts = datetime.datetime.strptime(day, '%Y-%m-%d').timestamp()
        end_ts = start_ts + 86399

        data = bin_data.get_data_bin(
            run_object['symbol'],
            int(start_ts*1000),
            int(end_ts*1000),
            run_object['period']
            )

        self.send_it_forward(data, run_object['period'])

        self.backfill_col.update_one(run_object, {'$set': {"state": 2}})

        return data
