from kafka import KafkaProducer
import json
import time


class PETROSASender(object):
    def __init__(self, topic):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self.topic = topic
        self.total_sent = 0

        self.producer.send(self.topic, b'HOLD THE LINE')
        print('Started Sender for: ', self.topic)

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
        except Exception as e:
            print(e)
            print(type(msg))
            print(msg)
            # raise #DEBUG
