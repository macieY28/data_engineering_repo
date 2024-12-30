# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC CREATE TABLE metadata_table (
# MAGIC     source_name STRING,
# MAGIC     last_processed_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'dbfs:/data_engineering_repo/cloudproject/data/metadata';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO hive_metastore.default.metadata_table (source_name, last_processed_date) VALUES
# MAGIC     ('weather', '1970-01-01T00:00:00'),
# MAGIC     ('flights', '1970-01-01T00:00:00');
