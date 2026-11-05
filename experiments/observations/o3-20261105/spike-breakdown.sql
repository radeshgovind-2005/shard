-- What is_demand_spike catches on 2025 (D28): spikes by the hour's count and by the bucket's mean.
-- Run: spark-sql --master local[2] -f spike-breakdown.sql (Docker master, over a copy of the 2025 O3 output).
CREATE TEMPORARY VIEW o3 USING parquet OPTIONS (path '/tmp/o3-local-2025');
SELECT count(*) AS rows, sum(CAST(is_demand_spike AS INT)) AS spikes, round(avg(CAST(is_demand_spike AS INT)), 4) AS spike_share FROM o3;
SELECT CASE WHEN hist_mean_trips < 0.5 THEN 'a: mean < 0.5' WHEN hist_mean_trips < 1 THEN 'b: 0.5-1' WHEN hist_mean_trips < 10 THEN 'c: 1-10' WHEN hist_mean_trips < 100 THEN 'd: 10-100' ELSE 'e: >= 100' END AS bucket_mean,
  count(*) AS rows, sum(CAST(is_demand_spike AS INT)) AS spikes, sum(CASE WHEN is_demand_spike AND trip_count = 1 THEN 1 ELSE 0 END) AS spikes_with_1_trip
FROM o3 GROUP BY 1 ORDER BY 1;
SELECT max(demand_index) AS max_di, percentile(demand_index, 0.5) AS p50_di, percentile(demand_index, 0.99) AS p99_di FROM o3;
SELECT PULocationID, service_zone, pickup_date, hour_of_day, trip_count, round(hist_mean_trips, 1) AS mean, round(demand_index, 2) AS di
FROM o3 WHERE is_demand_spike AND hist_mean_trips >= 100 ORDER BY demand_index DESC LIMIT 10;
