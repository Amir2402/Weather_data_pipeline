from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from plugins.operators.createBucketOperator import createBucketOperator
from plugins.operators.loadDataToBucketOperator import loadDataToBucketOperator
from plugins.operators.loadDailyWeatherData import LoadDailyWeatherData
from plugins.operators.transformAndWriteToSilver import transformAndWriteToSilver
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ENDPOINT_URL
from include.utils.todayWeatherCheck import weather_exist
from datetime import datetime

# Define the DAG
@dag(
    dag_id = "meda_pipeline", 
    start_date = datetime(2021, 10, 10),
    catchup = False,
    schedule = '@hourly'
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

    check_today_weather = BranchPythonOperator(
        task_id = "check_today_weather", 
        python_callable = weather_exist
    )

    skip_task = EmptyOperator(
        task_id = "skip_task"
    )

    transform_and_write_to_silver = transformAndWriteToSilver(
        task_id = "transform_and_write_to_silver",
        trigger_rule='one_success',  
        access_key = MINIO_ACCESS_KEY,
        secret_key = MINIO_SECRET_KEY, 
        today_date = datetime.now()
    )

    [create_bronze_bukcet, create_silver_bukcet, create_gold_bukcet] >> load_lat_long_to_bronze >> check_today_weather
    check_today_weather >> [load_daily_weather_data, skip_task] >> transform_and_write_to_silver
    
    


generate_dag()