from kafka import KafkaProducer, KafkaConsumer
from time import sleep
from json import loads

bootstrap_servers = 'localhost:9092'

def printResponse(response, seconds=1):
    print(f'{response.topic}:::partition {response.partition}/ offset {response.offset}/ timestamp {response.timestamp}')
    sleep(seconds)

def getProducer():
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                            value_serializer=lambda v: str(v).encode('utf-8'))

def getConsumer(group_id=None, client_id=None, max_poll_records=1):
    return KafkaConsumer(
        group_id=group_id,
        client_id=client_id,
        max_poll_records=max_poll_records,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda v: v.decode('utf-8')
    )
