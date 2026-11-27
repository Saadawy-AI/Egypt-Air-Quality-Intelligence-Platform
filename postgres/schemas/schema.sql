CREATE TABLE IF NOT EXISTS dim_source (
    source_key BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_governorate (
    governorate_key BIGSERIAL PRIMARY KEY,
    governorate_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_metric (
    metric_key BIGSERIAL PRIMARY KEY,
    metric_name TEXT NOT NULL,
    unit TEXT NOT NULL DEFAULT '',
    CONSTRAINT uq_dim_metric_name_unit UNIQUE (metric_name, unit)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_hourly_measurements (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    source_key BIGINT NOT NULL REFERENCES dim_source(source_key),
    governorate_key BIGINT NOT NULL REFERENCES dim_governorate(governorate_key),
    metric_key BIGINT NOT NULL REFERENCES dim_metric(metric_key),
    average_value DOUBLE PRECISION NOT NULL,
    minimum_value DOUBLE PRECISION NOT NULL,
    maximum_value DOUBLE PRECISION NOT NULL,
    measurement_count BIGINT NOT NULL CHECK (measurement_count > 0),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    latitude_count BIGINT NOT NULL DEFAULT 0,
    longitude_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, source_key, governorate_key, metric_key)
);

CREATE INDEX IF NOT EXISTS idx_fact_hourly_date
    ON fact_hourly_measurements (date_key, window_start);

CREATE INDEX IF NOT EXISTS idx_fact_hourly_metric
    ON fact_hourly_measurements (metric_key, window_start);

CREATE TABLE IF NOT EXISTS processed_gold_batches (
    batch_id BIGINT PRIMARY KEY,
    row_count BIGINT NOT NULL,
    source_path TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    source_key BIGINT NOT NULL REFERENCES dim_source(source_key),
    governorate_key BIGINT NOT NULL REFERENCES dim_governorate(governorate_key),
    metric_key BIGINT NOT NULL REFERENCES dim_metric(metric_key),
    observed_value DOUBLE PRECISION NOT NULL,
    baseline_median DOUBLE PRECISION NOT NULL,
    baseline_mad DOUBLE PRECISION NOT NULL CHECK (baseline_mad >= 0),
    deviation_score DOUBLE PRECISION NOT NULL CHECK (deviation_score >= 0),
    severity TEXT NOT NULL CHECK (severity IN ('warning', 'critical')),
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    telegram_sent_at TIMESTAMPTZ,
    telegram_message_id TEXT,
    suppressed_at TIMESTAMPTZ,
    suppression_reason TEXT,
    CONSTRAINT uq_anomaly_alert_series_hour
        UNIQUE (window_start, source_key, governorate_key, metric_key)
);


CREATE INDEX IF NOT EXISTS idx_anomaly_alerts_pending_v2
    ON anomaly_alerts (created_at, alert_id)
    WHERE telegram_sent_at IS NULL AND suppressed_at IS NULL;

-- Keep measured observations separate from meteorological/model-derived
-- values without rewriting or deleting any historical fact rows. OpenAQ and
-- ESP32 are observation sources. Open-Meteo values are model output (including
-- forecast horizons), not ground-station observations.
CREATE OR REPLACE VIEW fact_hourly_measurements_classified AS
SELECT
    f.*,
    s.source_name,
    CASE
        WHEN s.source_name IN ('openaq', 'esp32_pm_sensor') THEN 'observation'
        WHEN s.source_name IN ('open_meteo_air_quality', 'open_meteo_weather') THEN 'model_output'
        ELSE 'unclassified'
    END AS measurement_class
FROM fact_hourly_measurements AS f
JOIN dim_source AS s ON s.source_key = f.source_key;
