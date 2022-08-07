import boto3
import logging
from botocore.client import Config
from botocore.exceptions import ClientError
import os

s3 = boto3.client('s3',
                    endpoint_url='http://localhost:9000',
                    aws_access_key_id='minioadmin',
                    aws_secret_access_key='minioadmin',
                    config=Config(signature_version='s3v4'),
    )

def create_bucket(s3_client ,bucket_name):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    # Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_file(s3_client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name =  os.path.basename(file_name)
    else:
        object_name = object_name + os.path.basename(file_name)

    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def listing_bucket(s3_client):
    response = s3.list_buckets()
    print('Existing buckets:')
    for bucket in response['Buckets']:
        print(f'  {bucket["Name"]}')

def delete_object(s3, bucket, object_name):
    try:
        responce = s3.delete_object(
            Bucket = bucket,
            Key = object_name
        )
    except ClientError as e:
        logging.error(e)
        return False
    return True

def list_objects(s3, bucket_name):
    resp = s3.list_objects_v2(Bucket = bucket_name)
    for obj in resp['Contents']:
        files = obj['Key']
        print(files)
    return files


if __name__ == '__main__':  
    # result = create_bucket(s3, 'test-bucket')
    # print(result)

    listing_bucket(s3)
    # upload_file(s3, 'train.csv', 'test-bucket', 'test/')
    # delete_object(s3, 'test-bucket', 'csv/train.csv')
    list_objects(s3, bucket_name='test-bucket')