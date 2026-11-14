-- ===========================================================================
-- Gold schema -- Databricks SQL port (ACTIVE)
-- ===========================================================================
-- Ported from the original PostgreSQL DDL below (kept as a commented
-- reference). Translation notes:
--   TEXT          -> STRING
--   NUMERIC(p,s)  -> DECIMAL(p,s)
--   TIMESTAMPTZ   -> TIMESTAMP
-- PRIMARY KEY constraints (column-level and composite) are supported on
-- Delta tables as informational constraints. No column DEFAULTs here, so no
-- TBLPROPERTIES are needed. The live pipeline writes to
-- workspace.retail_nama_gold (Spark port in /src/build_gold.py); this DDL
-- defines the same structure under schema `gold` for parity with the
-- original /database/schemas definitions. All object names below are
-- fully qualified (workspace.gold.*) so they resolve in any SQL context
-- regardless of the session's default catalog.

CREATE SCHEMA IF NOT EXISTS workspace.gold;

CREATE TABLE IF NOT EXISTS workspace.gold.order_360 (
    order_id STRING PRIMARY KEY,
    customer_id STRING NOT NULL,
    order_date DATE NOT NULL,
    sales_channel STRING NOT NULL,
    store_id STRING,
    gross_merchandise_value DECIMAL(14, 2) NOT NULL,
    discount_amount DECIMAL(14, 2) NOT NULL,
    shipping_revenue DECIMAL(14, 2) NOT NULL,
    captured_payment_amount DECIMAL(14, 2) NOT NULL,
    refunded_amount DECIMAL(14, 2) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    item_count INTEGER NOT NULL,
    unit_quantity INTEGER NOT NULL,
    order_status STRING NOT NULL,
    payment_status STRING NOT NULL,
    return_status STRING NOT NULL,
    promotion_count INTEGER NOT NULL,
    first_order_flag BOOLEAN NOT NULL,
    pipeline_run_id STRING NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace.gold.customer_daily (
    customer_id STRING NOT NULL,
    metric_date DATE NOT NULL,
    order_count INTEGER NOT NULL,
    unit_quantity INTEGER NOT NULL,
    gross_revenue DECIMAL(14, 2) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    refund_amount DECIMAL(14, 2) NOT NULL,
    return_count INTEGER NOT NULL,
    support_contacts INTEGER NOT NULL,
    active_channel STRING,
    new_customer_flag BOOLEAN NOT NULL,
    repeat_customer_flag BOOLEAN NOT NULL,
    pipeline_run_id STRING NOT NULL,
    PRIMARY KEY (customer_id, metric_date)
);

CREATE TABLE IF NOT EXISTS workspace.gold.product_daily (
    product_id STRING NOT NULL,
    metric_date DATE NOT NULL,
    active_category STRING,
    units_sold INTEGER NOT NULL,
    order_count INTEGER NOT NULL,
    gross_revenue DECIMAL(14, 2) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    refunded_units INTEGER NOT NULL,
    available_inventory INTEGER,
    reserved_inventory INTEGER,
    stockout_flag BOOLEAN,
    promotion_count INTEGER NOT NULL,
    pipeline_run_id STRING NOT NULL,
    PRIMARY KEY (product_id, metric_date)
);

CREATE TABLE IF NOT EXISTS workspace.gold.channel_campaign_daily (
    metric_date DATE NOT NULL,
    sales_channel STRING NOT NULL,
    campaign_id STRING NOT NULL,
    campaign_spend DECIMAL(14, 2) NOT NULL,
    attributed_orders INTEGER NOT NULL,
    attributed_customers INTEGER NOT NULL,
    gross_revenue DECIMAL(14, 2) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    refunds DECIMAL(14, 2) NOT NULL,
    roas DECIMAL(14, 4),
    conversion_rate DECIMAL(14, 4),
    pipeline_run_id STRING NOT NULL,
    PRIMARY KEY (metric_date, sales_channel, campaign_id)
);

CREATE TABLE IF NOT EXISTS workspace.gold.executive_kpis_daily (
    metric_date DATE PRIMARY KEY,
    total_orders INTEGER NOT NULL,
    gross_revenue DECIMAL(14, 2) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    average_order_value DECIMAL(14, 2) NOT NULL,
    refund_rate DECIMAL(14, 4) NOT NULL,
    return_rate DECIMAL(14, 4) NOT NULL,
    repeat_customer_rate DECIMAL(14, 4) NOT NULL,
    stockout_rate DECIMAL(14, 4) NOT NULL,
    active_customers INTEGER NOT NULL,
    support_contact_rate DECIMAL(14, 4) NOT NULL,
    data_freshness_utc TIMESTAMP NOT NULL,
    pipeline_run_id STRING NOT NULL
);


-- ===========================================================================
-- Original PostgreSQL DDL (inactive, kept for reference)
-- ===========================================================================
-- (reference DDL converted from a block comment to line comments: the SQL
-- file task's statement splitter breaks on semicolons inside block comments)
-- CREATE SCHEMA IF NOT EXISTS gold;
--
-- CREATE TABLE IF NOT EXISTS gold.order_360 (
--     order_id TEXT PRIMARY KEY,
--     customer_id TEXT NOT NULL,
--     order_date DATE NOT NULL,
--     sales_channel TEXT NOT NULL,
--     store_id TEXT,
--     gross_merchandise_value NUMERIC(14, 2) NOT NULL,
--     discount_amount NUMERIC(14, 2) NOT NULL,
--     shipping_revenue NUMERIC(14, 2) NOT NULL,
--     captured_payment_amount NUMERIC(14, 2) NOT NULL,
--     refunded_amount NUMERIC(14, 2) NOT NULL,
--     net_revenue NUMERIC(14, 2) NOT NULL,
--     item_count INTEGER NOT NULL,
--     unit_quantity INTEGER NOT NULL,
--     order_status TEXT NOT NULL,
--     payment_status TEXT NOT NULL,
--     return_status TEXT NOT NULL,
--     promotion_count INTEGER NOT NULL,
--     first_order_flag BOOLEAN NOT NULL,
--     pipeline_run_id TEXT NOT NULL
-- );
--
-- CREATE TABLE IF NOT EXISTS gold.customer_daily (
--     customer_id TEXT NOT NULL,
--     metric_date DATE NOT NULL,
--     order_count INTEGER NOT NULL,
--     unit_quantity INTEGER NOT NULL,
--     gross_revenue NUMERIC(14, 2) NOT NULL,
--     net_revenue NUMERIC(14, 2) NOT NULL,
--     refund_amount NUMERIC(14, 2) NOT NULL,
--     return_count INTEGER NOT NULL,
--     support_contacts INTEGER NOT NULL,
--     active_channel TEXT,
--     new_customer_flag BOOLEAN NOT NULL,
--     repeat_customer_flag BOOLEAN NOT NULL,
--     pipeline_run_id TEXT NOT NULL,
--     PRIMARY KEY (customer_id, metric_date)
-- );
--
-- CREATE TABLE IF NOT EXISTS gold.product_daily (
--     product_id TEXT NOT NULL,
--     metric_date DATE NOT NULL,
--     active_category TEXT,
--     units_sold INTEGER NOT NULL,
--     order_count INTEGER NOT NULL,
--     gross_revenue NUMERIC(14, 2) NOT NULL,
--     net_revenue NUMERIC(14, 2) NOT NULL,
--     refunded_units INTEGER NOT NULL,
--     available_inventory INTEGER,
--     reserved_inventory INTEGER,
--     stockout_flag BOOLEAN,
--     promotion_count INTEGER NOT NULL,
--     pipeline_run_id TEXT NOT NULL,
--     PRIMARY KEY (product_id, metric_date)
-- );
--
-- CREATE TABLE IF NOT EXISTS gold.channel_campaign_daily (
--     metric_date DATE NOT NULL,
--     sales_channel TEXT NOT NULL,
--     campaign_id TEXT NOT NULL,
--     campaign_spend NUMERIC(14, 2) NOT NULL,
--     attributed_orders INTEGER NOT NULL,
--     attributed_customers INTEGER NOT NULL,
--     gross_revenue NUMERIC(14, 2) NOT NULL,
--     net_revenue NUMERIC(14, 2) NOT NULL,
--     refunds NUMERIC(14, 2) NOT NULL,
--     roas NUMERIC(14, 4),
--     conversion_rate NUMERIC(14, 4),
--     pipeline_run_id TEXT NOT NULL,
--     PRIMARY KEY (metric_date, sales_channel, campaign_id)
-- );
--
-- CREATE TABLE IF NOT EXISTS gold.executive_kpis_daily (
--     metric_date DATE PRIMARY KEY,
--     total_orders INTEGER NOT NULL,
--     gross_revenue NUMERIC(14, 2) NOT NULL,
--     net_revenue NUMERIC(14, 2) NOT NULL,
--     average_order_value NUMERIC(14, 2) NOT NULL,
--     refund_rate NUMERIC(14, 4) NOT NULL,
--     return_rate NUMERIC(14, 4) NOT NULL,
--     repeat_customer_rate NUMERIC(14, 4) NOT NULL,
--     stockout_rate NUMERIC(14, 4) NOT NULL,
--     active_customers INTEGER NOT NULL,
--     support_contact_rate NUMERIC(14, 4) NOT NULL,
--     data_freshness_utc TIMESTAMPTZ NOT NULL,
--     pipeline_run_id TEXT NOT NULL
-- );
--
