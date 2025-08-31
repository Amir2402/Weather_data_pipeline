from airflow.sdk import BaseOperator
import boto3
from botocore.client import Config
import requests as rq 
import json 
from datetime import datetime 

class LoadDailyWeatherData(BaseOperator):
    def __init__(self, access_key, secret_key, endpoint_url, **kwargs):
        super().__init__(**kwargs)
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url

    def execute(self, context):
        s3 = boto3.resource(
        "s3",
        endpoint_url=self.endpoint_url,
        aws_access_key_id=self.access_key,
        aws_secret_access_key=self.secret_key
        )
        URL = "https://api.open-meteo.com/v1/forecast"

        lat_long_list = self.get_latitude_longitude(s3)
        requests_results = self.get_data_from_weather_api(lat_long_list, URL)
        self.write_weather_data_to_s3(requests_results, s3)


    def get_latitude_longitude(self, s3): 
        print('test')
        lat_long_object = s3.Object('bronze', 'latitude_longitude.csv')
        file_content = lat_long_object.get()['Body'].read().decode('utf-8')
        lat_long_list = []

        for line in file_content.split('\n'): 
            line = line.split(',')
            lat_long_list.append((line[1], line[2]))

        return lat_long_list
    
    def get_data_from_weather_api(self, lat_long_list, url): 
        requests_results = []

        for lat, long in lat_long_list: 
            res = rq.get(url, params = {
                "latitude": lat,
                "longitude": long,
                "hourly": "temperature_2m",
                "forecast_days": 1,
            })
            print(res)
            requests_results.append(res.json())
        
        return requests_results  
    
    def write_weather_data_to_s3(self, requests_results, s3): 
        today_day = datetime.now().day
        today_month = datetime.now().month
        today_year = datetime.now().year

        s3object = s3.Object('bronze', f'daily_temp_data{today_month}_{today_day}_{today_year}.json')
        
        daily_temp_object = {
            'result': requests_results
        }

        s3object.put(
            Body=(bytes(json.dumps(daily_temp_object).encode('UTF-8'))),
            ContentType="application/json"
        )
