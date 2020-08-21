class Order(object):
    def __init__(self, user_id, order_id, amount):
        self.user_id = str(user_id)
        self.order_id = str(order_id)
        self.amount = amount
