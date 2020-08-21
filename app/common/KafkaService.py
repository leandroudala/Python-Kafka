from kafka import KafkaConsumer

class KafkaService:
    def parse(self, response):
        print(f'{response.topic}:::partition {response.partition}/ offset {response.offset}/ timestamp {response.timestamp}')
    
    def __init__(self, topic=None, group_id=None, client_id=None, max_poll_records=1, parse=None, pattern=None):
        self._consumer = KafkaConsumer(
            group_id=group_id,
            client_id=client_id,
            max_poll_records=max_poll_records,
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda v: v.decode('utf-8'),
            consumer_timeout_ms=400
        )
        if pattern is not None:
            self._consumer.subscribe(pattern=pattern)
        else:
            self._consumer.subscribe(topic)
        
        if parse:
            self.parse = parse
        
    def __enter__(self):
        return self

    def run(self):
        while True:
            for record in self._consumer:
                self.parse(record)

    def __exit__(self, a, b, c, d):
        self._consumer.close()
