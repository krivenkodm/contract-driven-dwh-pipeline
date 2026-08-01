-- Existing DDS rows predate the business DQ columns. Resetting only the
-- DDS source watermarks schedules a deterministic full recomputation on the
-- next build-dds run without modifying the append-only RAW layer.
UPDATE etl_watermarks
SET
    last_raw_id = 0,
    processed_dttm = CURRENT_TIMESTAMP
WHERE
    pipeline_name = 'dds_orders';
