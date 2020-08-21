from kafka import KafkaProducer
from time import sleep

class KafkaDispatcher:
    def __init__(self):
        self._producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: str(v).encode('utf-8')
        )

    def send(self, topic, value, key, seconds=3):
        if hasattr(value, '__dict__'):  
            value = value.__dict__

        if type(key) == str:
            key = key.encode()

        response = self._producer.send(topic, value, key).get()
        print(f'{response.topic}:::partition {response.partition}/ offset {response.offset}/ timestamp {response.timestamp}')
        sleep(seconds)

    def __exit__(self):
        self._producer.close()
        print('Bye from producer')
