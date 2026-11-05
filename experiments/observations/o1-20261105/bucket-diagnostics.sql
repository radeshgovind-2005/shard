-- O1 bucket diagnostics: an independent re-computation of O1's flags in plain SQL, plus what the pipeline doesn't
-- write (D21): bucket sizes, Samuelson's n <= 10 share, the largest buckets, flags by distance, the heavy tail, and
-- rows per shuffle partition. Not a measurement: correctness evidence and leads for spark-profiler.
--
-- Run in the Docker master (spark-sql, local mode), with ${files} = one month or the whole year, e.g.:
--   docker compose -f docker/docker-compose.yml cp experiments/observations/o1-20261105/bucket-diagnostics.sql \
--     spark-master:/tmp/o1-diag.sql
--   docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-sql --master 'local[4]' \
--     --driver-memory 2g --hivevar files='/data/trips/yellow_tripdata_2025-*.parquet' -S -f /tmp/o1-diag.sql
-- Output: one tab-separated block per SELECT, in the order below.

SET spark.sql.session.timeZone=UTC;

-- M1's five criteria (D1, D2: a null passenger_count is valid, any other null fails its criterion), then D15–D16.
CREATE TEMPORARY VIEW trips AS
SELECT PULocationID,
       hour(CAST(tpep_pickup_datetime AS TIMESTAMP_NTZ)) AS hour_of_day,
       CAST(trip_distance AS DOUBLE) AS trip_distance,
       CAST(fare_amount AS DOUBLE) / (1.609344 * CAST(trip_distance AS DOUBLE)) AS fare_per_km
FROM parquet.`${files}`
WHERE fare_amount > 0
  AND trip_distance > 0
  AND (passenger_count IS NULL OR passenger_count BETWEEN 1 AND 6)
  AND tpep_dropoff_datetime > tpep_pickup_datetime
  AND PULocationID BETWEEN 1 AND 263
  AND DOLocationID BETWEEN 1 AND 263;

-- D17, D18: the same window as WithBucketFareStats + WithFareAnomalyFlag.
CACHE TABLE flagged AS
SELECT *, coalesce(fare_per_km > mu + 3 * sd, false) AS is_anomaly
FROM (
  SELECT *, count(*) OVER w AS n, avg(fare_per_km) OVER w AS mu, stddev_samp(fare_per_km) OVER w AS sd
  FROM trips
  WINDOW w AS (PARTITION BY PULocationID, hour_of_day)
);

-- 1. Totals: must equal M1 kept_trips and Σ flagged_trips of <output>/o1. Share vs normal 0.135 % and Cantelli 10 %.
SELECT count(*) AS trips, count_if(is_anomaly) AS flagged, round(100 * count_if(is_anomaly) / count(*), 4) AS flagged_pct,
       count(DISTINCT PULocationID, hour_of_day) AS buckets
FROM flagged;

-- 2. Bucket sizes: buckets, trips and flags per size band. Samuelson: the n <= 10 band must have 0 flags.
SELECT CASE WHEN n = 1 THEN 'a n=1' WHEN n <= 10 THEN 'b 2-10' WHEN n <= 100 THEN 'c 11-100'
            WHEN n <= 1000 THEN 'd 101-1k' WHEN n <= 10000 THEN 'e 1k-10k' ELSE 'f >10k' END AS bucket_size,
       count(DISTINCT PULocationID, hour_of_day) AS buckets, count(*) AS trips, count_if(is_anomaly) AS flagged
FROM flagged GROUP BY 1 ORDER BY 1;

-- 3. The 15 largest buckets: zone, hour, trips, share of all trips, mean and σ in $/km, flags.
SELECT PULocationID, hour_of_day, n, round(100 * n / sum(n) OVER (), 3) AS pct_of_trips,
       round(mu, 3) AS mean_fare_per_km, round(sd, 3) AS stddev_fare_per_km, flagged
FROM (SELECT PULocationID, hour_of_day, first(n) AS n, first(mu) AS mu, first(sd) AS sd,
             count_if(is_anomaly) AS flagged
      FROM flagged GROUP BY PULocationID, hour_of_day)
ORDER BY n DESC LIMIT 15;

-- 4. Heavy tails: trips, flags and rate per distance band (miles). Tiny distances give huge per-km fares.
SELECT CASE WHEN trip_distance < 0.1 THEN 'a <0.1' WHEN trip_distance < 0.5 THEN 'b 0.1-0.5'
            WHEN trip_distance < 1 THEN 'c 0.5-1' WHEN trip_distance < 5 THEN 'd 1-5' ELSE 'e >=5' END AS distance_mi,
       count(*) AS trips, count_if(is_anomaly) AS flagged, round(100 * count_if(is_anomaly) / count(*), 4) AS flagged_pct
FROM flagged GROUP BY 1 ORDER BY 1;

-- 5. The fare_per_km distribution: median, p99, p99.9, max; and the median bucket σ / mean.
SELECT percentile_approx(fare_per_km, array(0.5, 0.99, 0.999)) AS p50_p99_p999, max(fare_per_km) AS max_fare_per_km,
       percentile_approx(sd / mu, 0.5) AS median_bucket_cv
FROM flagged;

-- 6. Rows per shuffle partition of the baseline window's Exchange: hashpartitioning(PULocationID, hour_of_day, 200)
--    is pmod(murmur3 hash, 200), which is what hash() computes. Before AQE coalescing.
SELECT count(*) AS partitions, min(rows) AS min_rows, percentile_approx(rows, 0.5) AS median_rows, max(rows) AS max_rows,
       round(max(rows) / percentile_approx(rows, 0.5), 2) AS max_over_median
FROM (SELECT pmod(hash(PULocationID, hour_of_day), 200) AS part, count(*) AS rows FROM flagged GROUP BY 1);
