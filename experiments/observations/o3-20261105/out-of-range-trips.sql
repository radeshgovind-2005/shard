-- Kept trips (M1's five criteria, written from scratch) whose pickup is outside the --years range (D24).
-- Run: spark-sql --master local[2] -f out-of-range-trips.sql (Docker master, session time zone UTC).
SELECT '2025-01' AS years, count(*) AS out_of_range FROM (SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-01.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263) WHERE pickup < TIMESTAMP_NTZ'2025-01-01 00:00:00' OR pickup >= TIMESTAMP_NTZ'2025-02-01 00:00:00';
SELECT '2025' AS years, count(*) AS out_of_range FROM (
SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-01.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-02.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-03.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-04.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-05.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-06.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-07.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-08.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-09.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-10.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-11.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-12.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
) WHERE pickup < TIMESTAMP_NTZ'2025-01-01 00:00:00' OR pickup >= TIMESTAMP_NTZ'2026-01-01 00:00:00';
SELECT year(pickup) AS pickup_year, count(*) AS trips FROM (
SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-01.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-02.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-03.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-04.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-05.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-06.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-07.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-08.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-09.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-10.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-11.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
  UNION ALL SELECT CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ) AS pickup FROM parquet.`/data/trips/yellow_tripdata_2025-12.parquet` WHERE fare_amount > 0 AND trip_distance > 0
  AND (passenger_count IS NULL OR (passenger_count >= 1 AND passenger_count <= 6))
  AND tpep_pickup_datetime IS NOT NULL AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263 AND DOLocationID BETWEEN 1 AND 263
) WHERE pickup < TIMESTAMP_NTZ'2025-01-01 00:00:00' OR pickup >= TIMESTAMP_NTZ'2026-01-01 00:00:00' GROUP BY 1 ORDER BY 1;
