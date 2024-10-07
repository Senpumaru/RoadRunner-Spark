import boto3
from botocore.client import Config

s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin',
                  region_name='us-east-1',
                  config=Config(s3={'addressing_style': 'path'}),
                  use_ssl=False)

# List buckets
response = s3.list_buckets()
print("Existing buckets:")
for bucket in response['Buckets']:
    print(f'  {bucket["Name"]}')

# Try to create a test object
bucket_name = 'test'
object_key = 'test-object.txt'
s3.put_object(Bucket=bucket_name, Key=object_key, Body='Hello, Iceberg!')
print(f"Created test object: s3://{bucket_name}/{object_key}")

# List objects in the bucket
response = s3.list_objects_v2(Bucket=bucket_name)
print(f"Objects in {bucket_name}:")
for obj in response.get('Contents', []):
    print(f'  {obj["Key"]}')