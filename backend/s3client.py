import datetime

import boto3
import sys
import json


class S3Client:
    BUCKET_NAME = 'transitquality2024'

    def __init__(self):
        self.client = boto3.client('s3')
        #self.s3 = boto3.resource('s3')
        #self.client = self.s3.Client()
        #self.bucket = self.s3.Bucket('transitquality2024')

    def write_api_response(self, ts: datetime.datetime, command: str, contents: str):
        day = ts.strftime('%Y%m%d')
        full = ts.strftime('%H%M%Sz')
        key = f'bustracker/raw/{command}/{day}/t{full}.json'
        response = self.client.put_object(
            Bucket=self.BUCKET_NAME,
            Body=contents.encode('utf-8'),
            Key=key
        )
        return response


if __name__ == "__main__":
    # test
    c = S3Client()
    c.write_api_response(datetime.datetime.now(), 'test', json.dumps({'test-response': sys.argv[1]}))
