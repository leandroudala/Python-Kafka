from app.common.KafkaService import KafkaService
from app.common.KafkaDispatcher import KafkaDispatcher

import json

class FraudDetectorService:
    topic_rejected = 'ECOMMERCE_ORDER_REJECTED'
    topic_approved = 'ECOMMERCE_ORDER_APPROVED'

    def isFraud(self, order):
        # simulating a fraud
        if int(order['amount']) > 4500:
            return True
        return False

    def parse(self, record):
        order = json.loads(record.value.replace('\'', '\"'))
        user_id = str(order['user_id'])
        print('----- Processing Order -----')
        if self.isFraud(order):
            print("❌ Order is a fraud!!! ", user_id, '\tAmount:', order['amount'])
            self.dispatcher.send(topic=self.topic_rejected, value=record, key=user_id)
        else:
            print("👍 Order Approved! ", user_id, '\tAmount:', order['amount'])
            self.dispatcher.send(topic=self.topic_approved, value=record, key=user_id)

    def __init__(self):
        self.dispatcher = KafkaDispatcher()


def run():
    print('Press Ctrl+c to stop')
    fraudDetector = FraudDetectorService()

    service = KafkaService(
        topic='ECOMMERCE_NEW_ORDER',
        group_id='FraudDetector',
        client_id='fraud-host01',
        parse=fraudDetector.parse
    )
    service.run()
