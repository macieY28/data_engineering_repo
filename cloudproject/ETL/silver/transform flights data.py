# Databricks notebook source
from pyspark.sql.functions import col, expr, to_timestamp, current_timestamp
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# COMMAND ----------

def transform_date_cols(df, col_list):
    for col in col_list:
        df = df.withColumn(col, to_timestamp(df[col]))
    return df

def null_to_zero(df, col_list):
    for col in col_list:
        df = df.fillna({col:0})
    return df

def null_to_unknown(df, col_list):
    for col in col_list:
        df = df.fillna({col: "unknown"})
    return df

def deduplicate_rows(df, key_list):
    df = df.dropDuplicates(key_list)
    return df

# COMMAND ----------

metadata_path = "dbfs:/data_engineering_repo/cloudproject/data/metadata"
df_metadata = spark.read.format("delta").load(metadata_path)

last_processed_date = df_metadata.filter(col("source_name") == "flights").select("last_processed_date").collect()[0][0]

# COMMAND ----------

text_cols_list = ["estArrivalAirport", "estDepartureAirport", "icao24"]
numeric_cols_list = ["arrivalAirportCandidatesCount", "departureAirportCandidatesCount", "estArrivalAirportHorizDistance", "estArrivalAirportVertDistance", "estDepartureAirportHorizDistance", "estDepartureAirportVertDistance"]
date_cols_list = ["date_fs", "date_ls"]
key_list = ["callsign", "location"]
output_cols_list = key_list + text_cols_list + date_cols_list + numeric_cols_list + ['year', 'month']

# COMMAND ----------

df_departures = spark.read.format("delta").option("startingTimestamp", last_processed_date).load("dbfs:/data_engineering_repo/cloudproject/data/bronze/departures")
departures_output_path = "dbfs:/data_engineering_repo/cloudproject/data/silver/departures"

df_departures = transform_date_cols(df_departures, date_cols_list)
df_departures = null_to_unknown(df_departures, text_cols_list)
df_departures = null_to_zero(df_departures, numeric_cols_list)
df_departures = deduplicate_rows(df_departures, key_list)
df_departures = df_departures.select(*output_cols_list)

df_departures.write.format("delta").mode("append").partitionBy("year", "month").save(departures_output_path)

# COMMAND ----------

df_arrivals = spark.read.format("delta").option("startingTimestamp", last_processed_date).load("dbfs:/data_engineering_repo/cloudproject/data/bronze/arrivals")
arrivals_output_path = "dbfs:/data_engineering_repo/cloudproject/data/silver/arrivals"

df_arrivals = transform_date_cols(df_arrivals, date_cols_list)
df_arrivals = null_to_unknown(df_arrivals, text_cols_list)
df_arrivals = null_to_zero(df_arrivals, numeric_cols_list)
df_arrivals = deduplicate_rows(df_arrivals, key_list)
df_arrivals = df_arrivals.select(*output_cols_list)

df_arrivals.write.format("delta").mode("append").partitionBy("year", "month").save(arrivals_output_path)

# COMMAND ----------

df_metadata = df_metadata.withColumn("last_processed_date", current_timestamp()).where("source_name = 'flights'")
delta_metadata = DeltaTable.forPath(spark, metadata_path)

delta_metadata.alias("target").merge(
    df_metadata.alias("source"),
    "target.source_name = source.source_name"  
).whenMatchedUpdate(set={
    "last_processed_date": "source.last_processed_date" 
}).execute()
