# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC
# MAGIC USE SCHEMA gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   date_trunc('hour', '2024-12-28T01:53:11.000+00:00') AS truncated_timestamp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gold.airport_traffic_by_hour AS
# MAGIC WITH arrivals_agg AS(
# MAGIC   SELECT 
# MAGIC     Location, 
# MAGIC     DATE_TRUNC('HOUR', ArrivalStart) AS ArrivalStart, 
# MAGIC     IFNULL(COUNT(Callsign),0) AS ArrivalsCount, 
# MAGIC     ROUND(AVG(ArrivalTime),2) AS AverageArrivalTime
# MAGIC   FROM hive_metastore.gold.arrivals
# MAGIC   GROUP BY Location, DATE_TRUNC('HOUR', ArrivalStart)
# MAGIC ),
# MAGIC departures_agg AS(
# MAGIC   SELECT
# MAGIC     Location,
# MAGIC     DATE_TRUNC('HOUR', DepartureStart) AS DepartureStart,
# MAGIC     IFNULL(COUNT(Callsign),0) AS DeparturesCount,
# MAGIC     ROUND(AVG(DepartureTime),2) AS AverageDepartureTime
# MAGIC   FROM hive_metastore.gold.departures
# MAGIC   GROUP BY Location, DATE_TRUNC('HOUR', DepartureStart)
# MAGIC )
# MAGIC SELECT
# MAGIC   wh.Location,
# MAGIC   wh.IntervalStart,
# MAGIC   wh.IntervalEnd,
# MAGIC   wh.TemperatureCelcius,
# MAGIC   wh.RelativeHumidity,
# MAGIC   wh.Rain,
# MAGIC   wh.Showers,
# MAGIC   wh.Snowfall,
# MAGIC   wh.CloudCover,
# MAGIC   wh.Visibility,
# MAGIC   wh.WindSpeed,
# MAGIC   ar.ArrivalsCount,
# MAGIC   dp.DeparturesCount,
# MAGIC   ar.AverageArrivalTime,
# MAGIC   dp.AverageDepartureTime
# MAGIC FROM hive_metastore.gold.weather_hourly wh
# MAGIC LEFT JOIN arrivals_agg ar
# MAGIC ON wh.location = ar.location AND wh.IntervalStart <= ar.ArrivalStart AND wh.IntervalEnd > ar.ArrivalStart
# MAGIC LEFT JOIN departures_agg dp
# MAGIC ON wh.location = dp.location AND wh.IntervalStart <= dp.DepartureStart AND wh.IntervalEnd > dp.DepartureStart

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW hive_metastore.gold.airport_traffic_daily AS
# MAGIC WITH arrivals_agg AS(
# MAGIC   SELECT 
# MAGIC     Location, 
# MAGIC     DATE(ArrivalEnd) AS Date, 
# MAGIC     IFNULL(COUNT(Callsign),0) AS ArrivalsCount, 
# MAGIC     ROUND(AVG(ArrivalTime),2) AS AverageArrivalTime, 
# MAGIC     ROUND(AVG(EstimatedDistance),2) AS AverageDistanceArr 
# MAGIC   FROM
# MAGIC    hive_metastore.gold.arrivals 
# MAGIC   GROUP BY location, DATE(ArrivalEnd)
# MAGIC ),
# MAGIC departures_agg AS(
# MAGIC   SELECT 
# MAGIC     Location, 
# MAGIC     DATE(DepartureEnd) AS Date, 
# MAGIC     IFNULL(COUNT(Callsign),0) AS DeparturesCount, 
# MAGIC     ROUND(AVG(DepartureTime),2) AS AverageDepartureTime,
# MAGIC     ROUND(AVG(EstimatedDistance),2) AS AverageDistanceDep
# MAGIC   FROM 
# MAGIC     hive_metastore.gold.departures 
# MAGIC   GROUP BY location, DATE(DepartureEnd)
# MAGIC )
# MAGIC SELECT
# MAGIC   wd.Location,
# MAGIC   wd.Date,
# MAGIC   wd.MaxTemperatureCelcius,
# MAGIC   wd.MinTemperatureCelcius,
# MAGIC   wd.RainSum,
# MAGIC   wd.SnowfallSum,
# MAGIC   wd.ShowersSum,
# MAGIC   wd.MaxWindSpeed,
# MAGIC   ar.ArrivalsCount,
# MAGIC   dp.DeparturesCount,
# MAGIC   ar.AverageArrivalTime,
# MAGIC   dp.AverageDepartureTime,
# MAGIC   ar.AverageDistanceArr,
# MAGIC   dp.AverageDistanceDep
# MAGIC FROM hive_metastore.gold.weather_daily wd
# MAGIC LEFT JOIN arrivals_agg ar
# MAGIC ON wd.Location = ar.Location AND wd.Date = ar.Date
# MAGIC LEFT JOIN departures_agg dp
# MAGIC ON wd.Location = dp.Location AND wd.Date = dp.Date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT to_date(ArrivalEnd) FROM hive_metastore.gold.arrivals

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM arrivals
