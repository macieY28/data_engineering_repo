# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_weather_hourly
# MAGIC USING DELTA
# MAGIC OPTIONS (
# MAGIC     path 'dbfs:/data_engineering_repo/cloudproject/data/silver/weather_hourly'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_weather_daily
# MAGIC USING DELTA
# MAGIC OPTIONS (
# MAGIC     path 'dbfs:/data_engineering_repo/cloudproject/data/silver/weather_daily'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_departures 
# MAGIC AS
# MAGIC SELECT
# MAGIC   callsign,
# MAGIC   location,
# MAGIC   estArrivalAirport,
# MAGIC   estDepartureAirport,
# MAGIC   icao24,
# MAGIC   date_fs,
# MAGIC   date_ls,
# MAGIC   CAST((UNIX_TIMESTAMP(date_ls)-UNIX_TIMESTAMP(date_fs)) AS INT) DepartureTime,
# MAGIC   SQRT(POW(estArrivalAirportHorizDistance,2) + POW(estArrivalAirportVertDistance,2)) AS estDistance
# MAGIC FROM
# MAGIC   delta.`dbfs:/data_engineering_repo/cloudproject/data/silver/departures`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silver_arrivals
# MAGIC AS
# MAGIC SELECT
# MAGIC   callsign,
# MAGIC   location,
# MAGIC   estArrivalAirport,
# MAGIC   estDepartureAirport,
# MAGIC   icao24,
# MAGIC   date_fs,
# MAGIC   date_ls,
# MAGIC   CAST((UNIX_TIMESTAMP(date_ls)-UNIX_TIMESTAMP(date_fs)) AS INT) ArrivalTime,
# MAGIC   SQRT(POW(estArrivalAirportHorizDistance,2) + POW(estArrivalAirportVertDistance,2)) AS estDistance
# MAGIC FROM
# MAGIC   delta.`dbfs:/data_engineering_repo/cloudproject/data/silver/arrivals`;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.gold.weather_hourly AS trg
# MAGIC USING silver_weather_hourly AS src
# MAGIC ON src.location = trg.location AND src.interval_start = trg.IntervalStart
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.TemperatureCelcius = src.temperature_2m,
# MAGIC     trg.RelativeHumidity = src.relative_humidity_2m,
# MAGIC     trg.Rain = src.rain,
# MAGIC     trg.Showers = src.showers,
# MAGIC     trg.Snowfall = src.snowfall,
# MAGIC     trg.CloudCover = src.cloud_cover,
# MAGIC     trg.Visibility = src.visibility,
# MAGIC     trg.WindSpeed = src.wind_speed_10m,
# MAGIC     trg.LastModifiedDate = CASE 
# MAGIC                          WHEN trg.TemperatureCelcius <> src.temperature_2m
# MAGIC                            OR trg.RelativeHumidity <> src.relative_humidity_2m
# MAGIC                            OR trg.Rain <> src.rain
# MAGIC                            OR trg.Showers <> src.showers
# MAGIC                            OR trg.Snowfall <> src.snowfall
# MAGIC                            OR trg.CloudCover <> src.cloud_cover
# MAGIC                            OR trg.Visibility <> src.visibility
# MAGIC                            OR trg.WindSpeed <> src.wind_speed_10m
# MAGIC                          THEN current_timestamp()
# MAGIC                          ELSE trg.LastModifiedDate
# MAGIC                        END
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (Location, IntervalStart, IntervalEnd, TemperatureCelcius, RelativeHumidity, Rain, Showers, Snowfall, CloudCover, Visibility,      WindSpeed, WindDirection, LastModifiedDate)
# MAGIC   VALUES (src.location, src.interval_start, src.interval_end, src.temperature_2m, src.relative_humidity_2m, src.rain, src.showers, src.snowfall, src.cloud_cover, src.visibility, src.wind_speed_10m, src.wind_direction_10m, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.gold.weather_daily AS trg
# MAGIC USING silver_weather_daily AS src
# MAGIC ON src.location = trg.Location AND src.date = trg.Date
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.MaxTemperatureCelcius = src.temperature_2m_max,
# MAGIC     trg.MinTemperatureCelcius = src.temperature_2m_min,
# MAGIC     trg.RainSum = src.rain_sum,
# MAGIC     trg.ShowersSum = src.showers_sum,
# MAGIC     trg.SnowfallSum = src.snowfall_sum,
# MAGIC     trg.MaxWindSpeed = src.wind_speed_10m_max,
# MAGIC     trg.LastModifiedDate = CASE 
# MAGIC                          WHEN trg.MaxTemperatureCelcius <> src.temperature_2m_max
# MAGIC                            OR trg.MinTemperatureCelcius <> src.temperature_2m_min
# MAGIC                            OR trg.RainSum <> src.rain_sum
# MAGIC                            OR trg.ShowersSum <> src.showers_sum
# MAGIC                            OR trg.SnowfallSum <> src.snowfall_sum
# MAGIC                            OR trg.MaxWindSpeed <> src.wind_speed_10m_max
# MAGIC                          THEN current_timestamp()
# MAGIC                          ELSE trg.LastModifiedDate
# MAGIC                        END
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (Location, Date, MaxTemperatureCelcius, MinTemperatureCelcius, RainSum, ShowersSum, SnowfallSum, MaxWindSpeed, LastModifiedDate)
# MAGIC   VALUES (src.location, src.date, src.temperature_2m_max, src.temperature_2m_min, src.rain_sum, src.showers_sum, src.snowfall_sum, src.wind_speed_10m_max, current_timestamp())
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.gold.departures AS trg
# MAGIC USING silver_departures AS src
# MAGIC ON src.callsign = trg.Callsign AND src.location = trg.Location
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.Callsign = src.callsign,
# MAGIC     trg.Location = src.location,
# MAGIC     trg.DestinationAirport = src.estArrivalAirport,
# MAGIC     trg.DepartureStart = src.date_fs,
# MAGIC     trg.DepartureEnd = src.date_ls,
# MAGIC     trg.DepartureTime = src.DepartureTime,
# MAGIC     trg.EstimatedDistance = src.estDistance,
# MAGIC     trg.LastModifiedDate = CASE 
# MAGIC                              WHEN trg.Callsign <> src.callsign
# MAGIC                                OR trg.Location <> src.location
# MAGIC                                OR trg.DestinationAirport <> src.estArrivalAirport
# MAGIC                                OR trg.DepartureStart <> src.date_fs
# MAGIC                                OR trg.DepartureEnd <> src.date_ls
# MAGIC                                OR trg.DepartureTime <> src.DepartureTime
# MAGIC                                OR trg.EstimatedDistance <> src.estDistance
# MAGIC                              THEN current_timestamp()
# MAGIC                              ELSE trg.LastModifiedDate
# MAGIC                            END
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (Callsign, Location, DepartureAirport, DestinationAirport, DepartureStart, DepartureEnd, DepartureTime, EstimatedDistance, LastModifiedDate)
# MAGIC   VALUES (src.callsign, src.location, src.estDepartureAirport, src.estArrivalAirport, src.date_fs, src.date_ls, src.DepartureTime, src.estDistance, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO hive_metastore.gold.arrivals AS trg
# MAGIC USING silver_arrivals AS src
# MAGIC ON src.callsign = trg.Callsign AND src.location = trg.Location
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.Callsign = src.callsign,
# MAGIC     trg.Location = src.location,
# MAGIC     trg.DestinationAirport = src.estArrivalAirport,
# MAGIC     trg.ArrivalStart = src.date_fs,
# MAGIC     trg.ArrivalEnd = src.date_ls,
# MAGIC     trg.ArrivalTime = src.ArrivalTime,
# MAGIC     trg.EstimatedDistance = src.estDistance,
# MAGIC     trg.LastModifiedDate =  CASE 
# MAGIC                              WHEN trg.Callsign <> src.callsign
# MAGIC                                OR trg.Location <> src.location
# MAGIC                                OR trg.DestinationAirport <> src.estArrivalAirport
# MAGIC                                OR trg.ArrivalStart <> src.date_fs
# MAGIC                                OR trg.ArrivalEnd <> src.date_ls
# MAGIC                                OR trg.ArrivalTime <> src.ArrivalTime
# MAGIC                                OR trg.EstimatedDistance <> src.estDistance
# MAGIC                              THEN current_timestamp()
# MAGIC                              ELSE trg.LastModifiedDate
# MAGIC                            END
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (Callsign, Location, DepartureAirport, DestinationAirport, ArrivalStart, ArrivalEnd, ArrivalTime, EstimatedDistance, LastModifiedDate)
# MAGIC   VALUES (src.callsign, src.location, src.estDepartureAirport, src.estArrivalAirport, src.date_fs, src.date_ls, src.ArrivalTime, src.estDistance, current_timestamp())
