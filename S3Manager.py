import boto3
from botocore.exceptions import ClientError
import logging

class S3Manager:
    def create_bucket(self, bucket_name, region=None):
        # Create bucket
        try:
            if region is None:
                s3_client = boto3.client('s3')
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client = boto3.client('s3', region_name=region)
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def upload_file(self, file_name, bucket, object_key):
        # If S3 key was not specified, use file_name
        if object_key is None:
            object_key = file_name
    
        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_key)
        except ClientError as e:
            logging.error(e)
            return False
        return True

        