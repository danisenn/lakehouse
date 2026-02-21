import boto3
from botocore.client import Config

s3_client = boto3.client(
    's3',
    endpoint_url='http://10.28.1.180:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    aws_session_token=None,
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    verify=False
)

response = s3_client.list_objects_v2(Bucket='datalake', Prefix='raw/nba/')
if 'Contents' in response:
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print('No objects found with prefix raw/nba/')
