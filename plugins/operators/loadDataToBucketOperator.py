from airflow.sdk import BaseOperator
import boto3
from botocore.client import Config

class loadDataToBucketOperator(BaseOperator):
    def __init__(self, bucket_name, object_path, object_name, access_key, secret_key, endpoint_url, **kwargs):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_path = object_path
        self.object_name = object_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

    def execute(self, context):
        s3 = boto3.client(
        "s3",
        endpoint_url=self.endpoint_url,
        aws_access_key_id=self.access_key,
        aws_secret_access_key=self.secret_key
        )
        try:
            s3.head_object(Bucket=self.bucket_name, Key=self.object_name)
            self.log.info(f"Key: '{self.object_name}' exists!")
        
        except Exception as e: 
            self.log.error(e)
            try: 
                s3.upload_file(self.object_path, self.bucket_name, self.object_name)
                self.log.info("object is uploaded")
                
            except Exception as e2: 
                self.log.error(f"an error occured: {e2}")

