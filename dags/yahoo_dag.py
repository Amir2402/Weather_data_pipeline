from airflow.decorators import dag
from plugins.operators.createBucketOperator import createBucketOperator
from plugins.operators.loadDataToBucketOperator import loadDataToBucketOperator
from plugins.operators.loadDailyWeatherData import LoadDailyWeatherData
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ENDPOINT_URL
from datetime import datetime

# Define the DAG
@dag(
    dag_id = "meda_pipeline", 
    start_date = datetime(2021, 10, 10),
    catchup = False
)
def generate_dag(): 
    create_bronze_bukcet = createBucketOperator(
        task_id = "create_bronze_bucket",
        bucket_name = "bronze", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = ENDPOINT_URL
    )

    create_silver_bukcet = createBucketOperator(
        task_id = "create_silver_bucket",
        bucket_name = "silver", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = ENDPOINT_URL
    )

    create_gold_bukcet = createBucketOperator(
        task_id = "create_gold_bucket",
        bucket_name = "gold", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = ENDPOINT_URL
    )

    load_lat_long_to_bronze = loadDataToBucketOperator(
        task_id = "load_lat_long_data_to_bronze", 
        bucket_name = "bronze", 
        object_path = "include/data/latitude_longitude_data.csv",
        object_name = "latitude_longitude.csv",
        access_key = MINIO_ACCESS_KEY,
        secret_key = MINIO_SECRET_KEY,
        endpoint_url = ENDPOINT_URL
    )

    load_daily_weather_data = LoadDailyWeatherData(
        task_id = "load_daily_weather_data", 
        access_key = MINIO_ACCESS_KEY, 
        secret_key = MINIO_SECRET_KEY, 
        endpoint_url = ENDPOINT_URL
    )

    [create_bronze_bukcet >> create_silver_bukcet >>  create_gold_bukcet] >> load_lat_long_to_bronze >> load_daily_weather_data

generate_dag()