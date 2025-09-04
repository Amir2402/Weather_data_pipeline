def read_data_into_weather_table(today_year, today_month, today_day): 
    read_data_into_weather_table = f"""
        CREATE TABLE weather_table AS 
            SELECT 
                UNNEST(result) AS weather_data
            FROM 
                read_json('s3://bronze/daily_temp_data_{today_month}_{today_day}_{today_year}.json');
    """
    return read_data_into_weather_table

def filter_weather_view(current_hour): 
    filtered_weather = f"""
        CREATE VIEW weather_data_view AS
            SELECT * 
            FROM (
                SELECT
                    weather_data.latitude,
                    weather_data.longitude, 
                    weather_data.utc_offset_seconds, 
                    weather_data.timezone, 
                    weather_data.timezone_abbreviation, 
                    weather_data.elevation, 
                    CAST(UNNEST(weather_data.hourly.time) AS TIMESTAMP) AS temperature_datetime, 
                    UNNEST(weather_data.hourly.temperature_2m) AS temperature
                FROM 
                    weather_table)
            WHERE 
                hour(temperature_datetime) = {current_hour};
    """
    return filtered_weather

def write_to_silver_layer(current_year, current_month, current_day, current_hour):
    write_to_silver_query = f"""
        COPY joined_lat_long_weather_table 
        TO 's3://silver/silver_weather_data/{current_year}/{current_month}/{current_day}/{current_hour}.parquet' (FORMAT parquet);
    """
    return write_to_silver_query

def read_silver_data(current_year, current_month, current_day, current_hour):
    silver_data = f"""
        CREATE TABLE weather_table AS   
            SELECT 
                *   
            FROM 
                read_parquet('s3://silver/silver_weather_data/{current_year}/{current_month}/{current_day}/{current_hour}.parquet')
    """
    return silver_data

def write_to_gold(table_name, current_year, current_month, current_day, current_hour):
    write_to_gold_query = f"""
        COPY {table_name} 
        TO 's3://gold/{table_name}/{current_year}/{current_month}/{current_day}/{current_hour}.parquet' (FORMAT parquet);
    """
    return write_to_gold_query

read_data_into_lat_long_table = """
    CREATE TABLE lat_long_table AS 
        SELECT 
            *
        FROM 
            read_csv('s3://bronze/latitude_longitude.csv', header = true);
    """

joined_weather_country = """
    CREATE TABLE joined_lat_long_weather_table AS 
        SELECT 
            *
        FROM 
            weather_data_view wdv
        JOIN 
            lat_long_table llt 
        ON 
            wdv.latitude = llt.latitude; 
    """

country_count = """
    CREATE TABLE country_count_table AS 
        SELECT 
            COUNT(country) AS country_count
        FROM    
            weather_table; 
    """

countries_weather_rank = """
    CREATE TABLE countries_weather_rank_table AS  
        SELECT  
            country, 
            temperature, 
            RANK() OVER(ORDER BY temperature DESC) AS country_rank 
        FROM  
            weather_table; 
    """

average_temperature = """
    CREATE TABLE average_temperature_table AS 
    SELECT 
        AVG(temperature)
    FROM 
        weather_table; 
    """

temperature_average_differece = """
    CREATE TABLE temperature_average_difference_table AS
    SELECT 
        country, 
        temperature,
        temperature - AVG(temperature) OVER(), 
    FROM 
        weather_table; 
    """