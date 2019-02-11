import json
import boto3

from sqs.config import QUEUE_URL, ACCESS_KEY, SECRET_KEY, AWS_REGION, AWS_SERVICE

sqs = boto3.client(
    AWS_SERVICE,
    region_name=AWS_REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)


def send_report_results():
    for i in range(1, 7):
        body = json.dumps({'jobId': 'test01', 'data': {'topping': [
            {'id': '5001', 'type': 'Chery'},
            {'id': '5002', 'type': 'Glazed'},
            {'id': '5005', 'type': 'Sugar'},
            {'id': '5007', 'type': 'Powdered Sugar'},
            {'id': '5006', 'type': 'Chocolate with Sprinkles'},
            {'id': '5003', 'type': 'Chocolate'},
            {'id': '5004', 'type': 'Maple'}]
        }})
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=body
        )
        print(response)
        print('SUCCESS')


send_report_results()
