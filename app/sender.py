import json
import logging
import os
import sys
import time

import newrelic.agent
from kafka import KafkaProducer


class PETROSASender(object):
    def __init__(self, topic):
        self.producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_ADDRESS', 'localhost:9093')
            )
        self.topic = topic
        self.total_sent = 0

        logging.warning('Kafka Brokers : ' + os.getenv('KAFKA_ADDRESS', 'localhost:9093'))
        logging.warning('Started Sender for: ' +  self.topic)
        self.start_time = time.time()
        self.last_time_shown = 0

    # Here we create a dual interface for list and for dict
    # Some subscriptions responds different than others, using lists or dicts


    @newrelic.agent.background_task()
    def to_send(self, msg):
        if(type(msg) is list):
            for _msg in msg:
                self.send(_msg)
        else:
            self.send(msg)


    @newrelic.agent.background_task()
    def send(self, msg) -> None:
        try:
            
            msg['petrosa_timestamp'] = time.time()

            msg = json.dumps(msg)
            msg = bytes(msg, encoding='utf8')

            self.producer.send(self.topic, msg)
            self.total_sent += 1
        except Exception as e:
            logging.error(e)
            sys.exit(1)
            