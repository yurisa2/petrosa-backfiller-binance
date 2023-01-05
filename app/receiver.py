from kafka import KafkaConsumer
import os
import json
import threading
import newrelic.agent
import logging

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
            print('Error in Kafka Consumer')
            os._exit(1)

        threading.Thread(target=self.run).start()

        print('Started receiver on topic ', topic)



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
            os._exit(1)
        

        return True
