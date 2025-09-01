from airflow.sdk import BaseOperator
from datetime import datetime 
from include.utils.duckdbS3Connection import duckcdS3Connection
import boto3
import json 
import duckdb 

class transformAndWriteToSilver(BaseOperator):
    def __init__(self, access_key, secret_key, **kwargs):
        super().__init__(**kwargs)
        self.conn = duckcdS3Connection(access_key, secret_key)
    
    def execute(): 
        pass
