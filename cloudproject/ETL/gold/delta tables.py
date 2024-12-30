# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.departures(
# MAGIC     Callsign STRING,
# MAGIC     Location STRING,
# MAGIC     DepartureAirport STRING,
# MAGIC     DestinationAirport STRING,
# MAGIC     DepartureStart TIMESTAMP,
# MAGIC     DepartureEnd TIMESTAMP,
# MAGIC     DepartureTime INT,
# MAGIC     EstimatedDistance DECIMAL(10,2),
# MAGIC     LastModifiedDate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/data_engineering_repo/cloudproject/data/gold/departures"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.arrivals(
# MAGIC     Callsign STRING,
# MAGIC     Location STRING,
# MAGIC     DepartureAirport STRING,
# MAGIC     DestinationAirport STRING,
# MAGIC     ArrivalStart TIMESTAMP,
# MAGIC     ArrivalEnd TIMESTAMP,
# MAGIC     ArrivalTime INT,
# MAGIC     EstimatedDistance DECIMAL(10,2),
# MAGIC     LastModifiedDate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/data_engineering_repo/cloudproject/data/gold/arrivals"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.weather_hourly(
# MAGIC     Location STRING,
# MAGIC     IntervalStart TIMESTAMP,
# MAGIC     IntervalEnd TIMESTAMP,
# MAGIC     TemperatureCelcius DECIMAL(10,2),
# MAGIC     RelativeHumidity DECIMAL(10,2),
# MAGIC     Rain DECIMAL(10,2),
# MAGIC     Showers DECIMAL(10,2),
# MAGIC     Snowfall DECIMAL(10,2),
# MAGIC     CloudCover INT,
# MAGIC     Visibility INT,
# MAGIC     WindSpeed DECIMAL(10,2),
# MAGIC     WindDirection DECIMAL(10,2),
# MAGIC     LastModifiedDate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/data_engineering_repo/cloudproject/data/gold/weather_hourly"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.weather_daily(
# MAGIC   Location STRING,
# MAGIC   Date DATE,
# MAGIC   MaxTemperatureCelcius DECIMAL(10,2),
# MAGIC   MinTemperatureCelcius DECIMAL(10,2),
# MAGIC   RainSum DECIMAL(10,2),
# MAGIC   ShowersSum DECIMAL(10,2),
# MAGIC   SnowfallSum DECIMAL(10,2),
# MAGIC   MaxWindSpeed DECIMAL(10,2),
# MAGIC   LastModifiedDate TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION "dbfs:/data_engineering_repo/cloudproject/data/gold/weather_daily"
# MAGIC
