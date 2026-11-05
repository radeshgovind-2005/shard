-- Local vs Docker O3 output, 2025-01. Keys and integer columns must match exactly; doubles within 1e-12 relative.
-- Run: spark-sql --master local[2] -f local-vs-docker-o3.sql (Docker master).
CREATE TEMPORARY VIEW l USING parquet OPTIONS (path '/tmp/o3-local-2025-01');
CREATE TEMPORARY VIEW d USING parquet OPTIONS (path '/output/dev-o3/o3');
SELECT (SELECT count(*) FROM l) AS local_rows, (SELECT count(*) FROM d) AS docker_rows;
SELECT count(*) AS exact_except_all_l_minus_d FROM (SELECT * FROM l EXCEPT ALL SELECT * FROM d);
SELECT count(*) AS exact_except_all_d_minus_l FROM (SELECT * FROM d EXCEPT ALL SELECT * FROM l);
SELECT count(*) AS matched,
  sum(CASE WHEN l.service_zone <=> d.service_zone AND l.day_of_week = d.day_of_week AND l.trip_count = d.trip_count
            AND l.lag_4w_trip_count <=> d.lag_4w_trip_count AND l.is_demand_spike = d.is_demand_spike THEN 0 ELSE 1 END) AS int_mismatches,
  max(abs(l.hist_mean_trips - d.hist_mean_trips) / greatest(abs(l.hist_mean_trips), 1e-300)) AS max_rel_mean,
  max(abs(l.hist_var_trips - d.hist_var_trips) / greatest(abs(l.hist_var_trips), 1e-300)) AS max_rel_var,
  max(abs(l.demand_index - d.demand_index) / greatest(abs(l.demand_index), 1e-300)) AS max_rel_di,
  sum(CASE WHEN (l.demand_index IS NULL) <> (d.demand_index IS NULL) THEN 1 ELSE 0 END) AS di_null_mismatches
FROM l JOIN d ON l.PULocationID = d.PULocationID AND l.pickup_date = d.pickup_date AND l.hour_of_day = d.hour_of_day;
