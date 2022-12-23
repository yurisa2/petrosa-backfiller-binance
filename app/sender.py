from kafka import KafkaProducer
import json
import time
import os
import logging
import sys


class PETROSASender(object):
    def __init__(self, topic):
        self.producer = KafkaProducer(
                bootstrap_servers=os.getenv('KAFKA_ADDRESS', 'localhost:9092')
            )
        self.topic = topic
        self.total_sent = 0

        logging.warning('Kafka Brokers : ' + os.getenv('KAFKA_ADDRESS', 'localhost:9092'))
        logging.warning('Started Sender for: ' +  self.topic)
        self.start_time = time.time()
        self.last_time_shown = 0

    # Here we create a dual interface for list and for dict
    # Some subscriptions responds different than others, using lists or dicts

    def to_send(self, msg):
        if(type(msg) is list):
            for _msg in msg:
                self.send(_msg)
        else:
            self.send(msg)

    def send(self, msg):
        try:
            msg['petrosa_timestamp'] = time.time()

            msg = json.dumps(msg)
            msg = bytes(msg, encoding='utf8')

            self.producer.send(self.topic, msg)
            self.total_sent += 1
            
            if (time.time() - self.last_time_shown > 60):
                logging.warning(str(self.total_sent) + " sent since start")
                self.last_time_shown = time.time()
                
        except Exception as e:
            logging.error(e)
            sys.exit()
