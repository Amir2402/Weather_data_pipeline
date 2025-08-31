import requests as rq 
import json
import boto3
from datetime import datetime 

MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
ENDPOINT_URL = "http://minio:9000"

URL = "https://api.open-meteo.com/v1/forecast"

s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
        )
lat_long_object = s3.Object('bronze', 'latitude_longitude.csv')
file_content = lat_long_object.get()['Body'].read().decode('utf-8')

lat_long_list = []

for line in file_content.split('\n'): 
    line = line.split(',')
    lat_long_list.append((line[1], line[2]))

request_results = []

for lat, long in lat_long_list[:2]: 
    res = rq.get(URL, params = {
        "latitude": lat,
        "longitude": long,
        "hourly": "temperature_2m",
        "forecast_days": 1,
    })
    print(res)
    request_results.append(res.json())

today_day = datetime.now().day
today_month = datetime.now().month
today_year = datetime.now().year

s3object = s3.Object('bronze', f'daily_temp_data{today_month}_{today_day}_{today_year}.json')
 
daily_temp_object = {
    'result': request_results
}

s3object.put(
    Body=(bytes(json.dumps(daily_temp_object).encode('UTF-8'))),
    ContentType="application/json"
)
