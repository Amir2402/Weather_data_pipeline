import duckdb 

def duckcdS3Connection(minio_access_key, minio_secret_key):
    con = duckdb.connect()
    con.install_extension("httpfs")
    con.load_extension("httpfs")
    minio_endpoint = 'minio:9000'

    con.execute(f"SET s3_access_key_id='{minio_access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    con.execute(f"SET s3_endpoint='{minio_endpoint}';")
    con.execute(f"SET s3_use_ssl=false;")
    con.execute(f"SET s3_url_style='path';")

    return con