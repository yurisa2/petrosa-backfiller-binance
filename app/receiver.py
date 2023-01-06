import json
import logging
import os
import sys
import threading

import newrelic.agent
from kafka import KafkaConsumer


class PETROSAReceiver(object):
    def __init__(self,
                 topic,
                 msg_queue
                 ) -> None:
        try:
            self.consumer = KafkaConsumer(topic,
                                bootstrap_servers=os.getenv(
                                    'KAFKA_SUBSCRIBER', 'localhost:9093'),
                                group_id='petrosa-backfiller-binance'
                                )
            self.msg_queue = msg_queue
        except:
            logging.error('Error in Kafka Consumer')
            sys.exit(1)

        threading.Thread(target=self.run).start()

        logging.warning('Started receiver on topic ', topic)


    @newrelic.agent.background_task()
    def run(self):
        try:
            counter = 0

            for msg in self.consumer:
                msg = msg.value.decode()
                msg = json.loads(msg)
                self.msg_queue.put(msg)
                
        except Exception as e:
            logging.error(e)
            logging.error('Error in Kafka Consumer')
            sys.exit(1)
        

        return True
