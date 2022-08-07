import boto3
import logging
from botocore.exceptions import ClientError

def getConnection(host, access_key_id, secret_key) -> boto3.client:
    secure = False
    s3 = boto3.client('s3',endpoint_url = host,aws_access_key_id = access_key_id,aws_secret_access_key = secret_key)
    
    return s3

def create_bucket(s3_client, bucket_name):
    try:
        s3_client.create_bucket(Bucket = bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file_to_bucket(s3,
                file_name,
                bucket,
                object_name):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        s3.create_bucket(Bucket = bucket)
    
    try:
        s3.upload_file(file_name, bucket, object_name)
    except clientError as e:
        logging.error(e)
        return False
    
    #upload Check
    resp = s3.list_objects_v2(Bucket = bucket)
    for obj in resp['Contents']:
        files = obj['Key']
    print(files)

if __name__ == "__main__":
    
    
    target = getConnection(
        host = 'http://localhost:9001',
        access_key_id = 'minioadmin',
        secret_key = 'minioadmin'
    )
    
    source = getConnection(
        host = 'http://localhost:9000',
        access_key_id = 'minioadmin',
        secret_key = 'minioadmin'
        )
    
    buckets = source.list_buckets()
    for bucket in buckets['Buckets']:
        print(bucket['Name'])
    
    
    #데이터 읽기 테스트
    # source.download_file(
    #     Bucket = 'test-bucket',
    #     Key="test/train.csv",
    #     Filename = './temp/temp.csv',
    # )
   

        
        
    upload_file(
        target,
        './temp/temp.csv',
        'target-test',
        'temp/temp.csv'
    )
    
        
