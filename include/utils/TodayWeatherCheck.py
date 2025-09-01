import boto3
from datetime import datetime 
from botocore.exceptions import ClientError
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ENDPOINT_URL 

def weather_exist():  
    today_date = datetime.now() 
    today_day = today_date.day
    today_month = today_date.month
    today_year = today_date.year 

    object_name = f"daily_temp_data_{today_month}_{today_day}_{today_year}.json"
    bucket_name = "bronze"

    try: 
        s3 = boto3.client(
            "s3",
            endpoint_url = ENDPOINT_URL,
            aws_access_key_id = MINIO_ACCESS_KEY,
            aws_secret_access_key = MINIO_SECRET_KEY
            )
        s3.head_object(Bucket = bucket_name, Key = object_name)
        return "skip_task" 
    
    except ClientError: 
        return 'load_daily_weather_data'
    
    except Exception as e: 
        print("an error occured: ", e)

