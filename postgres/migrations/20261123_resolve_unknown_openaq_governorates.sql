-- One-time correction for OpenAQ fact rows previously assigned Unknown.
-- Classification was verified against the 27 CAPMAS Admin-1 polygons in data/reference/egypt_governorates_capmas.geojson (EPSG:4326).
-- Idempotent: only Unknown OpenAQ facts with these exact observed coordinates are selected.
BEGIN;

WITH classified AS (
    SELECT
        f.*,
        CASE
            WHEN abs(f.latitude - 30.099407670574287) < 0.00000001
             AND abs(f.longitude - 31.63850168272893) < 0.00000001 THEN 'Cairo'
            WHEN abs(f.latitude - 30.04147) < 0.00000001
             AND abs(f.longitude - 31.23289) < 0.00000001 THEN 'Cairo'
            WHEN abs(f.latitude - 31.23378) < 0.00000001
             AND abs(f.longitude - 30.04102) < 0.00000001 THEN 'Alexandria'
        END AS resolved_governorate
    FROM fact_hourly_measurements f
    JOIN dim_source s ON s.source_key = f.source_key
    JOIN dim_governorate g ON g.governorate_key = f.governorate_key
    WHERE s.source_name = 'openaq'
      AND g.governorate_name = 'Unknown'
), grouped AS (
    SELECT
        c.window_start,
        c.window_end,
        c.date_key,
        c.source_key,
        target.governorate_key,
        c.metric_key,
        sum(c.average_value * c.measurement_count) / nullif(sum(c.measurement_count), 0) AS average_value,
        min(c.minimum_value) AS minimum_value,
        max(c.maximum_value) AS maximum_value,
        sum(c.measurement_count)::bigint AS measurement_count,
        sum(coalesce(c.latitude, 0) * c.latitude_count) / nullif(sum(c.latitude_count), 0) AS latitude,
        sum(coalesce(c.longitude, 0) * c.longitude_count) / nullif(sum(c.longitude_count), 0) AS longitude,
        sum(c.latitude_count)::bigint AS latitude_count,
        sum(c.longitude_count)::bigint AS longitude_count
    FROM classified c
    JOIN dim_governorate target ON target.governorate_name = c.resolved_governorate
    WHERE c.resolved_governorate IS NOT NULL
    GROUP BY c.window_start, c.window_end, c.date_key, c.source_key,
             target.governorate_key, c.metric_key
)
INSERT INTO fact_hourly_measurements (
    window_start, window_end, date_key, source_key, governorate_key,
    metric_key, average_value, minimum_value, maximum_value,
    measurement_count, latitude, longitude, latitude_count, longitude_count
)
SELECT window_start, window_end, date_key, source_key, governorate_key,
       metric_key, average_value, minimum_value, maximum_value,
       measurement_count, latitude, longitude, latitude_count, longitude_count
FROM grouped
ON CONFLICT (window_start, source_key, governorate_key, metric_key)
DO UPDATE SET
    window_end = EXCLUDED.window_end,
    date_key = EXCLUDED.date_key,
    average_value = (
        fact_hourly_measurements.average_value * fact_hourly_measurements.measurement_count
        + EXCLUDED.average_value * EXCLUDED.measurement_count
    ) / (fact_hourly_measurements.measurement_count + EXCLUDED.measurement_count),
    minimum_value = LEAST(fact_hourly_measurements.minimum_value, EXCLUDED.minimum_value),
    maximum_value = GREATEST(fact_hourly_measurements.maximum_value, EXCLUDED.maximum_value),
    measurement_count = fact_hourly_measurements.measurement_count + EXCLUDED.measurement_count,
    latitude = CASE WHEN fact_hourly_measurements.latitude_count + EXCLUDED.latitude_count = 0 THEN NULL ELSE
        (coalesce(fact_hourly_measurements.latitude, 0) * fact_hourly_measurements.latitude_count
         + coalesce(EXCLUDED.latitude, 0) * EXCLUDED.latitude_count)
        / (fact_hourly_measurements.latitude_count + EXCLUDED.latitude_count) END,
    longitude = CASE WHEN fact_hourly_measurements.longitude_count + EXCLUDED.longitude_count = 0 THEN NULL ELSE
        (coalesce(fact_hourly_measurements.longitude, 0) * fact_hourly_measurements.longitude_count
         + coalesce(EXCLUDED.longitude, 0) * EXCLUDED.longitude_count)
        / (fact_hourly_measurements.longitude_count + EXCLUDED.longitude_count) END,
    latitude_count = fact_hourly_measurements.latitude_count + EXCLUDED.latitude_count,
    longitude_count = fact_hourly_measurements.longitude_count + EXCLUDED.longitude_count,
    updated_at = now();

DELETE FROM fact_hourly_measurements f
USING dim_source s, dim_governorate g
WHERE f.source_key = s.source_key
  AND f.governorate_key = g.governorate_key
  AND s.source_name = 'openaq'
  AND g.governorate_name = 'Unknown'
  AND (
      (abs(f.latitude - 30.099407670574287) < 0.00000001 AND abs(f.longitude - 31.63850168272893) < 0.00000001)
      OR (abs(f.latitude - 30.04147) < 0.00000001 AND abs(f.longitude - 31.23289) < 0.00000001)
      OR (abs(f.latitude - 31.23378) < 0.00000001 AND abs(f.longitude - 30.04102) < 0.00000001)
  );

COMMIT;
