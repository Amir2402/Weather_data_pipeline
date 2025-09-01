import duckdb 
from plugins.helpers.variables import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, ENDPOINT_URL

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


df = con.execute("""
    SELECT * 
    FROM read_json('s3://bronze/daily_temp_data_8_31_2025.json'); 
""").fetchdf()

print(df)