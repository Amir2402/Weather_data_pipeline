from airflow.sdk import BaseOperator
from plugins.operators.transformAndWriteToSilver import transformAndWriteToSilver
from include.utils.queries import (write_to_gold, country_count, countries_weather_rank, 
                                   read_silver_data, average_temperature, temperature_average_differece)

class transformAndLoadToGold(BaseOperator): 
    def __init__(self, access_key, secret_key, today_date,**kwargs):
        super().__init__(**kwargs)
        self.conn = transformAndWriteToSilver.connect_duck_db_to_S3(self, access_key, secret_key)
        self.current_date = today_date
        self.current_day =  self.current_date.day
        self.current_month = self.current_date.month 
        self.current_year = self.current_date.year 
        self.current_hour = self.current_date.hour
    
    def execute(self, context): 
        self.conn.sql(read_silver_data(self.current_year, self.current_month,
                                       self.current_day, self.current_hour))
        self.log.info('read weather data from silver layer')

        self.conn.sql(country_count)
        self.log.info('created country_count_table!')
        self.conn.sql(write_to_gold('country_count_table', self.current_year, self.current_month, self.current_day, self.current_hour))
        self.log.info('loaded country_count_table to gold layer')

        self.conn.sql(countries_weather_rank)
        self.log.info('created countries_weather_rank_table!')
        self.conn.sql(write_to_gold('countries_weather_rank_table', self.current_year, self.current_month, self.current_day, self.current_hour))
        self.log.info('loaded countries_weather_rank_table to gold layer')

        self.conn.sql(average_temperature)
        self.log.info('created average_temperature_table!')
        self.conn.sql(write_to_gold('average_temperature_table', self.current_year, self.current_month, self.current_day, self.current_hour))
        self.log.info('loaded average_temperature_table to gold layer')

        self.conn.sql(temperature_average_differece)
        self.log.info('created temperature_average_difference_table!')
        self.conn.sql(write_to_gold('temperature_average_difference_table', self.current_year, self.current_month, self.current_day, self.current_hour))
        self.log.info('loaded temperature_average_difference_table to gold layer')


