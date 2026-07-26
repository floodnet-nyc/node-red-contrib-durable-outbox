CREATE TABLE IF NOT EXISTS sensor_readings (
    device_id   TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    value_mm    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (device_id, observed_at)
);
