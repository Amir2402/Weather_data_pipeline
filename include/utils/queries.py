read_data_into_weather_table = """
    CREATE TABLE weather_table AS 
    SELECT 
        UNNEST(result) AS weather_data
    FROM 
        read_json('s3://bronze/daily_temp_data_8_31_2025.json');
"""

read_data_into_lat_long = """
    CREATE TABLE lat_long_table AS 
    SELECT 
        *
    FROM 
        read_csv('s3://bronze/latitude_longitude.csv', header = true);
"""
def filtered_weather_view(current_hour): 
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

joined_weather_country = """
    SELECT 
        *
    FROM 
        weather_data_view wdv
    JOIN 
        lat_long_table llt 
    ON 
        wdv.latitude = llt.latitude; 
"""

