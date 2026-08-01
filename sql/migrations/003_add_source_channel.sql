ALTER TABLE raw_order_created
    ADD COLUMN IF NOT EXISTS source_channel varchar;


ALTER TABLE dds_orders
    ADD COLUMN IF NOT EXISTS source_channel varchar;