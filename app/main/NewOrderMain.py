from app.common.KafkaDispatcher import KafkaDispatcher
from .models.Order import Order

import uuid
import random
import json

def run():
    dispatcher = KafkaDispatcher()
    count = 500 * random.randint(1, 100)
    
    print("Press Ctrl+c to Stop")
    while True:
        user_id = uuid.uuid4()
        order_id = uuid.uuid4()
        amount = random.randint(1, 5000)

        order = Order(user_id=user_id, order_id=order_id, amount=amount)
        dispatcher.send('ECOMMERCE_NEW_ORDER', order, order.user_id)

        # dispatcher.send('ECOMMERCE_SEND_EMAIL', f'to {count}.{chr(value%27+65)}@example.com \nThank you. Your order is ok', key)
        count += 1
