import duckdb 
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ENDPOINT_URL
from include.utils.queries import read_data_into_weather_table, read_data_into_lat_long, filtered_weather_view, joined_weather_country
from datetime import datetime 

con = duckdb.connect()
con.install_extension("httpfs")
con.load_extension("httpfs")

minio_access_key_id = MINIO_ACCESS_KEY
minio_secret_access_key = MINIO_SECRET_KEY
minio_endpoint = ENDPOINT_URL

con.execute(f"SET s3_access_key_id='{minio_access_key_id}';")
con.execute(f"SET s3_secret_access_key='{minio_secret_access_key}';")
con.execute(f"SET s3_endpoint='minio:9000'")
con.execute(f"SET s3_use_ssl=false;")
con.execute(f"SET s3_url_style='path';")

df = con.sql(read_data_into_weather_table)

con.sql(filtered_weather_view(datetime.now().hour))


con.sql(read_data_into_lat_long)

con.sql("select * from weather_data_view").show()