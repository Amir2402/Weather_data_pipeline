from airflow.sdk import BaseOperator
from include.utils.queries import (read_data_into_weather_table, filter_weather_view,
                                   read_data_into_lat_long_table, joined_weather_country,
                                   write_to_silver_layer)
import duckdb 

class transformAndWriteToSilver(BaseOperator):
    def __init__(self, access_key, secret_key, today_date,**kwargs):
        super().__init__(**kwargs)
        self.conn = self.connect_duck_db_to_S3(access_key, secret_key)
        self.current_date = today_date
        self.current_day =  self.current_date.day
        self.current_month = self.current_date.month 
        self.current_year = self.current_date.year 
        self.current_hour = self.current_date.hour
    
    def execute(self, context): 
        self.conn.sql(read_data_into_weather_table(self.current_year, self.current_month, self.current_day))
        self.log.info("ingested weather data from bronze layer") 

        self.conn.sql(read_data_into_lat_long_table)
        self.log.info("ingested latitude and longitude data from bronze layer") 

        self.conn.sql(filter_weather_view(self.current_hour))
        self.log.info("flatenned json data and filtered with current hour")
        
        self.conn.sql(joined_weather_country)
        self.log.info("joined latitude longitude table with weather table")
        
        self.conn.execute(write_to_silver_layer(self.current_year, self.current_month, self.current_day, self.current_hour))
        self.log.info("loaded weather data to S3 successfully!")
    
    def connect_duck_db_to_S3(self, access_key, secret_key):
        conn = duckdb.connect()
        conn.install_extension("httpfs")
        conn.load_extension("httpfs")
        minio_endpoint = 'minio:9000'

        conn.execute(f"SET s3_access_key_id='{access_key}';")
        conn.execute(f"SET s3_secret_access_key='{secret_key}';")
        conn.execute(f"SET s3_endpoint='{minio_endpoint}';")
        conn.execute(f"SET s3_use_ssl=false;")
        conn.execute(f"SET s3_url_style='path';")

        return conn



        
