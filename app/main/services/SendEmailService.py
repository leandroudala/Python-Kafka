from app.common.KafkaService import KafkaService

class EmailService:
    def parse(self, record):
        print('----------------------')
        print('Topic:', record.topic)
        print('Key:', record.key)
        print('Value:', record.value)
        print(f'Partition: {record.partition}', f'Offset {record.offset}', sep='\t\t', end='\n\n')

def run():
    print("Press Ctrl+c to Stop")
    emailService = EmailService()
    service = KafkaService(
        topic='ECOMMERCE_SEND_EMAIL', 
        parse=emailService.parse, 
        group_id='EmailService', 
        client_id='email-host01'
    )
    service.run()
