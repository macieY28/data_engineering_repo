# Databricks notebook source
# MAGIC %md
# MAGIC **API configuration**

# COMMAND ----------

# MAGIC %pip install openmeteo-requests
# MAGIC %pip install requests-cache retry-requests numpy pandas

# COMMAND ----------

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import json

# COMMAND ----------

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest data from OpenMeteo API**

# COMMAND ----------

# Loading locations config file
def load_locations(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

# Function to ingest hourly intervals data
def ingest_hourly_data(url, params, locations, output_path):
    for location in locations:
        params["latitude"] = location["latitude"]
        params["longitude"] = location["longitude"]
        print(f"Fetching hourly data for {location['name']}...")

        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_rain = hourly.Variables(2).ValuesAsNumpy()
        hourly_showers = hourly.Variables(3).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(5).ValuesAsNumpy()
        hourly_visibility = hourly.Variables(6).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(7).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(8).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
            end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = hourly.Interval()),
            inclusive = "left"
        )}
        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["rain"] = hourly_rain
        hourly_data["showers"] = hourly_showers
        hourly_data["snowfall"] = hourly_snowfall
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["visibility"] = hourly_visibility
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["location"] = location["name"]
        hourly_data["latitude"] = location["latitude"]
        hourly_data["longitude"] = location["longitude"]

        df_hourly = pd.DataFrame(data = hourly_data)
        df_hourly["year"] = df_hourly["date"].dt.year
        df_hourly["month"] = df_hourly["date"].dt.month
        df_hourly["day"] = df_hourly["date"].dt.day

        df_hourly_spark = spark.createDataFrame(df_hourly)
        df_hourly_spark.write.format("delta").mode("append").partitionBy("year", "month").save(output_path)

#Function to ingest daily data
def ingest_daily_data(url, params, locations, output_path):
    for location in locations:
        params["latitude"] = location["latitude"]
        params["longitude"] = location["longitude"]
        print(f"Fetchin daily data for {location['name']}...")

        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation {response.Elevation()} m asl")
        print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")
        
        daily = response.Daily()
        daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
        daily_sunrise = daily.Variables(2).ValuesAsNumpy()
        daily_sunset = daily.Variables(3).ValuesAsNumpy()
        daily_rain_sum = daily.Variables(4).ValuesAsNumpy()
        daily_showers_sum = daily.Variables(5).ValuesAsNumpy()
        daily_snowfall_sum = daily.Variables(6).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(7).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
            )}
        daily_data["temperature_2m_max"] = daily_temperature_2m_max
        daily_data["temperature_2m_min"] = daily_temperature_2m_min
        daily_data["sunrise"] = daily_sunrise
        daily_data["sunset"] = daily_sunset
        daily_data["rain_sum"] = daily_rain_sum
        daily_data["showers_sum"] = daily_showers_sum
        daily_data["snowfall_sum"] = daily_snowfall_sum
        daily_data["wind_speed_10m_max"] = daily_wind_speed_10m_max
        daily_data["location"] = location["name"]
        daily_data["latitude"] = location["latitude"]
        daily_data["longitude"] = location["longitude"]

        df_daily = pd.DataFrame(data = daily_data)
        df_daily["year"] = df_daily["date"].dt.year
        df_daily["month"] = df_daily["date"].dt.month
        df_daily["day"] = df_daily["date"].dt.day

        df_daily_spark = spark.createDataFrame(df_daily)
        df_daily_spark.write.format("delta").mode("append").partitionBy("year", "month").save(output_path)


# COMMAND ----------

file_path = "/Workspace/Users/maciekcz95@outlook.com/data_engineering_repo/cloudproject/ETL/locations.json"
url = "https://api.open-meteo.com/v1/forecast"
params = {
	"latitude": 0.00000,
	"longitude": 0.00000,
	"hourly": ["temperature_2m", "relative_humidity_2m", "rain", "showers", "snowfall", "cloud_cover", "visibility", "wind_speed_10m", "wind_direction_10m"],
	"daily": ["temperature_2m_max", "temperature_2m_min", "sunrise", "sunset", "rain_sum", "showers_sum", "snowfall_sum", "wind_speed_10m_max"],
	"past_days": 1
}
locations = load_locations(file_path)
hourly_output_path = "dbfs:/data_engineering_repo/cloudproject/data/bronze/weather_hourly"
daily_output_path = "dbfs:/data_engineering_repo/cloudproject/data/bronze/weather_daily"

ingest_hourly_data(url, params, locations, hourly_output_path)
print("Ingest hourly data successfull")

ingest_daily_data(url, params, locations, daily_output_path)
print("Ingest daily data successfull")
