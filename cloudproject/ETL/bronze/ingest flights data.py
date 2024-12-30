# Databricks notebook source
# MAGIC %md
# MAGIC **API configuration**

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir -p /tmp/opensky-api
# MAGIC git clone https://github.com/openskynetwork/opensky-api.git /tmp/opensky-api

# COMMAND ----------

import sys
sys.path.append('/tmp/opensky-api/python/')

from opensky_api import OpenSkyApi

# COMMAND ----------

# MAGIC %md
# MAGIC **Ingest data from OpenSky API**

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, current_date, date_sub, lit, from_unixtime, year, month, day
import datetime
import json

# COMMAND ----------

api = OpenSkyApi()

def get_departures_by_airport(airports, begin, end, output_path):
    for airport in airports:
        airport_icao = airport['icao']
        departures = api.get_departures_by_airport(airport_icao, begin, end)
        print(f"Fetching departures for {airport['name']}...")
        if departures is None:
            print(f"No departures returned for {airport['name']} ({airport_icao}). Skipping...")
            continue
        try:
            df_departures = spark.createDataFrame(departures)
            df_departures = df_departures.withColumn("date_fs", from_unixtime(df_departures["firstSeen"]))
            df_departures = df_departures.withColumn("date_ls", from_unixtime(df_departures["lastSeen"]))
            df_departures = df_departures.withColumn("year", year(df_departures["date_fs"]))
            df_departures = df_departures.withColumn("month", month(df_departures["date_fs"]))
            df_departures = df_departures.withColumn("day", day(df_departures["date_fs"]))
            df_departures = df_departures.withColumn("location", lit(airport["name"]))
            df_departures.write.format("delta").mode("append").partitionBy("year", "month").save(output_path)
        except Exception as e:
            print(f"Error processing departures for {airport['name']} ({airport_icao}): {e}")

def get_arrivals_by_airport(airports, begin, end, output_path):
    for airport in airports:
        airport_icao = airport['icao']
        arrivals = api.get_arrivals_by_airport(airport_icao, begin, end)
        print(f"Fetching arrivals for {airport['name']}...")
        if arrivals is None:
            print(f"No arrivals returned for {airport['name']} ({airport_icao}). Skipping...")
            continue
        try:
            df_arrivals = spark.createDataFrame(arrivals)
            df_arrivals = df_arrivals.withColumn("date_fs", from_unixtime(df_arrivals["firstSeen"]))
            df_arrivals = df_arrivals.withColumn("date_ls", from_unixtime(df_arrivals["lastSeen"]))
            df_arrivals = df_arrivals.withColumn("year", year(df_arrivals["date_fs"]))
            df_arrivals = df_arrivals.withColumn("month", month(df_arrivals["date_fs"]))
            df_arrivals = df_arrivals.withColumn("day", day(df_arrivals["date_fs"]))
            df_arrivals = df_arrivals.withColumn("location", lit(airport["name"]))
            df_arrivals.write.format("delta").mode("append").partitionBy("year", "month").save(output_path)
        except Exception as e:
            print(f"Error processing arrivals for {airport['name']} ({airport_icao}): {e}")
    
def load_airports(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)


# COMMAND ----------

file_path = "/Workspace/Users/maciekcz95@outlook.com/data_engineering_repo/cloudproject/ETL/airports.json"
start_unix = spark.sql("SELECT unix_timestamp(date_sub(current_date(), 1), 'yyyy-MM-dd HH:mm:ss')").collect()[0][0]
end_unix = spark.sql("SELECT unix_timestamp(concat(date_sub(current_date(), 1), ' 23:59:59'), 'yyyy-MM-dd HH:mm:ss')").collect()[0][0]
airports = load_airports(file_path)
dep_output_path = "dbfs:/data_engineering_repo/cloudproject/data/bronze/departures"
arr_output_path = "dbfs:/data_engineering_repo/cloudproject/data/bronze/arrivals"

get_departures_by_airport(airports, start_unix, end_unix, dep_output_path)
print("Ingest departures data successfull")

get_arrivals_by_airport(airports, start_unix, end_unix, arr_output_path)
print("Ingest arrivals data successfull")
