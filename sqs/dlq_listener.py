import time
import boto3

from sqs.config import DLQ_URL, ACCESS_KEY, SECRET_KEY, AWS_REGION, AWS_SERVICE

sqs = boto3.client(
    AWS_SERVICE,
    region_name=AWS_REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

if __name__ == '__main__':
    print('STARTING DLQ WORKER listening on {}'.format(DLQ_URL))
    while True:
        response = sqs.receive_message(
            QueueUrl=DLQ_URL,
            AttributeNames=['All'],
            MessageAttributeNames=[
                'string',
            ],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        messages = response.get('Messages', [])
        for message in messages:
            print('Message Body > ', message.get('Body'))
            sqs.delete_message(QueueUrl=DLQ_URL, ReceiptHandle=message.get('ReceiptHandle'))

        time.sleep(10)

print('DLQ WORKER STOPPED')
