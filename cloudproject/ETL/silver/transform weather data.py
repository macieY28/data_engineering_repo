# Databricks notebook source
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# COMMAND ----------

def transform_numeric_cols(df, col_list):
    decimal_type = DecimalType(10,2)
    for col in col_list:
        df = df.withColumn(col, df[col].cast(decimal_type))
    return df

def null_to_zero(df, col_list):
    for col in col_list:
        df = df.fillna({col:0})
    return df

def deduplicate_rows(df, key_list):
    df = df.dropDuplicates(key_list)
    return df

# COMMAND ----------

metadata_path = "dbfs:/data_engineering_repo/cloudproject/data/metadata"
df_metadata = spark.read.format("delta").load(metadata_path)

last_processed_date = df_metadata.filter(col("source_name") == "weather").select("last_processed_date").collect()[0][0]

# COMMAND ----------

df_daily = spark.read.format("delta").option("startingTimestamp", last_processed_date).load("dbfs:/Users/maciekcz95@outlook.com/data_engineering_repo/cloudproject/ETL/bronze/weather_daily")
daily_output_path = "dbfs:/data_engineering_repo/cloudproject/data/silver/weather_daily"

daily_col_list = ["temperature_2m_max", "temperature_2m_min", "rain_sum", "showers_sum", "snowfall_sum", "wind_speed_10m_max"]
daily_key_list = ["date", "location"]

df_daily = transform_numeric_cols(df_daily, daily_col_list)
df_daily = null_to_zero(df_daily, daily_col_list)
df_daily = deduplicate_rows(df_daily, daily_key_list)

df_daily.write.format("delta").mode("append").partitionBy("year", "month").save(daily_output_path)

# COMMAND ----------

df_hourly = spark.read.format("delta").option("startingTimestamp", last_processed_date).load("dbfs:/Users/maciekcz95@outlook.com/data_engineering_repo/cloudproject/ETL/bronze/weather_hourly")
hourly_decimal_col = ["temperature_2m", "rain", "showers", "snowfall", "wind_speed_10m", "wind_direction_10m"]
hourly_col_list = ["temperature_2m", "relative_humidity_2m", "rain", "showers", "snowfall", "cloud_cover", "visibility", "wind_speed_10m", "wind_direction_10m"]
hourly_key_list = ["date", "location"]
hourly_output_path = "dbfs:/data_engineering_repo/cloudproject/data/silver/weather_hourly"

df_hourly = transform_numeric_cols(df_hourly, hourly_decimal_col)
df_hourly = null_to_zero(df_hourly, hourly_col_list)
df_hourly = deduplicate_rows(df_hourly, hourly_key_list)
df_hourly = df_hourly.withColumn("interval_end",expr("date + INTERVAL 1 HOUR"))
df_hourly = df_hourly.withColumnRenamed("date", "interval_start")

df_hourly.write.format("delta").mode("append").partitionBy("year", "month").save(hourly_output_path)

# COMMAND ----------

df_metadata = df_metadata.withColumn("last_processed_date", current_timestamp()).where("source_name = 'weather'")
delta_metadata = DeltaTable.forPath(spark, metadata_path)

delta_metadata.alias("target").merge(
    df_metadata.alias("source"),
    "target.source_name = source.source_name"  
).whenMatchedUpdate(set={
    "last_processed_date": "source.last_processed_date" 
}).execute()
