from app.common.KafkaService import KafkaService

class LogService:
    def parse(self, record):
        print(record.topic, record.key)

def run():
    print("Press Ctrl+C to stop")
    with KafkaService(
        pattern='ECOMMERCE.*',
        group_id='SYSTEM_LOG',
        client_id='LOCALHOST'
    ) as service:
        service.run()
