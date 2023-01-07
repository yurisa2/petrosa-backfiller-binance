import json
import logging
import os
import threading

import newrelic.agent
from kafka import KafkaConsumer


class PETROSAReceiver(object):
    def __init__(self,
                 topic,
                 msg_queue
                 ) -> None:
        self.consumer = KafkaConsumer(topic,
                            bootstrap_servers=os.getenv(
                                'KAFKA_SUBSCRIBER', 'localhost:9093'),
                            group_id='petrosa-backfiller-binance'
                            )
        self.msg_queue = msg_queue

        self.receiver = threading.Thread(target=self.run, daemon=True)
        self.receiver.start()

        logging.warning('Started receiver on topic ' + topic)


    @newrelic.agent.background_task()
    def run(self):
        counter = 0

        for msg in self.consumer:
            msg = msg.value.decode()
            msg = json.loads(msg)
            self.msg_queue.put(msg)
                
        return True
