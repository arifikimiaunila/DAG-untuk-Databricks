-- Databricks SQL compatibility notes (2026-11-14, updated after job-run failure):
--   - v_rejected_summary grouped by sales_channel/rejection_reason: those
--     columns exist in BOTH this DDL and the pipeline's runtime
--     silver_rejected_records schema (stage5 rewrites it via write_table)
--   - silver_status_map seed upserted via MERGE (Postgres ON CONFLICT
--     equivalent) so repeated runs stay idempotent
--   - silver_data_quality_events defined once (17-col version); no DROP so
--     pipeline DQ event history persists across runs
--   - ::casts in the operational views -> CAST(... AS ...)
--   - GROUP BY ordinals -> explicit column names
--   - Types are already Databricks-compatible (STRING/DECIMAL/TIMESTAMP)
-- ==========================================
-- SCHEMA DEFINITION
-- ==========================================
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id STRING,
    dag_id STRING,
    status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

-- =========================================================================
-- DDL: OPS.SILVER_ANOMALY_LOG
-- =========================================================================

CREATE TABLE IF NOT EXISTS ops.silver_anomaly_log (
    anomaly_id     STRING,
    check_name     STRING,
    target_table   STRING,
    table_name     STRING,
    issue_type     STRING,
    description    STRING,
    details_json   STRING,
    recorded_at    TIMESTAMP
);

-- (silver_data_quality_events is defined once, in section 7 below)

-- ==========================================
-- 1. INFRASTRUCTURE & PIPELINE ORCHESTRATION
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.drive_files_raw (
    id STRING,
    file_id STRING,
    file_name STRING,
    mime_type STRING,
    size_bytes BIGINT,
    modified_time TIMESTAMP,
    md5_checksum STRING,
    discovered_at TIMESTAMP,
    status STRING
);

CREATE TABLE IF NOT EXISTS ops.silver_processing_log (
    log_id STRING,
    pipeline_stage STRING,
    target_table STRING,
    records_in INT,
    records_out INT,
    records_rejected INT,
    status STRING,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.write_silver (
    write_id STRING,
    ops_log_id BIGINT,
    pipeline_stage STRING,
    target_table STRING,
    records_in INT,
    records_out INT,
    records_rejected INT,
    records_deduped INT,
    records_orphan INT,
    records_late_resolved INT,
    status STRING,
    business_date DATE,
    bronze_file_name STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    written_by STRING,
    message STRING,
    total_item_discount DECIMAL(14,2),
    total_shipping_cost DECIMAL(14,2),
    distinct_sales_channel INT
);

-- ==========================================
-- 2. LINEAGE & AUDITING LOGS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_lineage (
    lineage_id               STRING,
    deterministic_key        STRING,
    business_key             STRING,
    order_id                 STRING,
    customer_id              STRING,
    product_id               STRING,
    payment_id               STRING,
    event_id                 STRING,
    event_type               STRING,
    city_id                  STRING,
    store_id                 STRING,
    channel_id               STRING,
    address_id               STRING,
    order_item_id            STRING,
    promotion_id             STRING,
    campaign_id              STRING,
    refund_id                STRING,
    return_id                STRING,
    ticket_id                STRING,
    amount                   DECIMAL(14,2),
    shipping_revenue         DECIMAL(14,2),
    quantity                 DECIMAL(14,2),
    order_status             STRING,
    payment_status           STRING,
    status                   STRING,
    source_table             STRING,
    target_table             STRING,
    bronze_file_name         STRING,
    bronze_row_number        INT,
    bronze_source_record_id  STRING,
    bronze_checksum          STRING,
    bronze_ingested_at       TIMESTAMP,
    file_name                STRING,
    payload_json             STRING,
    job_name                 STRING,
    marquez_run_id           STRING,
    recorded_at              TIMESTAMP,
    marquez_facets           STRING
);

CREATE TABLE IF NOT EXISTS ops.silver_timestamp_audit (
    audit_id STRING,
    target_table STRING,
    source_record_id STRING,
    event_id STRING,
    original_field STRING,
    original_value STRING,
    normalized_utc_value TIMESTAMP,
    timezone_detected STRING,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_status_audit (
    audit_id STRING,
    target_table STRING,
    source_record_id STRING,
    event_id STRING,
    order_id STRING,
    original_status STRING,
    normalized_status STRING,
    sales_channel STRING,
    store_id STRING,
    channel_id STRING,
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_dedup_log (
    dedup_id STRING,
    target_table STRING,
    deterministic_key STRING,
    source_record_id STRING,
    payment_id STRING,
    event_id STRING,
    order_id STRING,
    address_id STRING,
    city_id STRING, 
    product_id STRING, 
    promotion_id STRING,
    channel_id STRING,
    store_id STRING,
    campaign_id STRING, 
    file_name STRING,
    row_number INT,
    is_kept BOOLEAN,
    duplicate_count INT,
    processed_at TIMESTAMP,
    item_discount_amount DECIMAL(14,2),
    shipping_cost DECIMAL(14,2),
    sales_channel STRING
);

-- ==========================================
-- 3. REFERENCE & STATUS MAP
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_status_map (
    raw_status STRING,
    normalized_status STRING
);

MERGE INTO ops.silver_status_map AS m
USING (
    SELECT * FROM (VALUES
    ('pending', 'pending'), ('PENDING', 'pending'), ('Pending', 'pending'),
    ('placed', 'pending'), ('PLACED', 'pending'), ('Placed', 'pending'),
    ('confirmed', 'pending'), ('CONFIRMED', 'pending'), ('Confirmed', 'pending'),
    ('completed', 'completed'), ('COMPLETED', 'completed'), ('Completed', 'completed'),
    ('fulfilled', 'completed'), ('FULFILLED', 'completed'), ('Fulfilled', 'completed'),
    ('paid', 'completed'), ('PAID', 'completed'), ('Paid', 'completed'),
    ('cancelled', 'cancelled'), ('CANCELLED', 'cancelled'), ('Cancelled', 'cancelled'),
    ('canceled', 'cancelled'), ('CANCELED', 'cancelled'), ('Canceled', 'cancelled'),
    ('returned', 'returned'), ('RETURNED', 'returned'), ('Returned', 'returned'),
    ('return', 'returned'), ('RETURN', 'returned'), ('Return', 'returned'),
    ('refunded', 'refunded'), ('REFUNDED', 'refunded'), ('Refunded', 'refunded'),
    ('failed', 'failed'), ('FAILED', 'failed'), ('Failed', 'failed')
    ) AS v(raw_status, normalized_status)
) AS s
ON m.raw_status = s.raw_status
WHEN MATCHED THEN UPDATE SET m.normalized_status = s.normalized_status
WHEN NOT MATCHED THEN INSERT (raw_status, normalized_status) VALUES (s.raw_status, s.normalized_status);

-- ==========================================
-- 4. STATE MANAGEMENT & IDENTITY RESOLUTION
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_event_identity (
    identity_id STRING,
    target_table STRING,
    event_business_key STRING,
    event_id STRING,
    event_type STRING,
    order_id STRING,
    payment_id STRING,
    refund_id STRING,
    return_id STRING,
    resolved_id STRING,
    competing_event_ids ARRAY<STRING>,
    resolution_rule STRING,
    winning_status STRING,
    payload_json STRING,
    resolved_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_temporal_versions (
    version_id STRING,
    target_table STRING,
    business_key STRING,
    event_id STRING,
    order_id STRING,
    version_number INT,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    valid_from_utc TIMESTAMP,
    valid_to_utc TIMESTAMP,
    source_record_id STRING,
    bronze_file_name STRING,
    payload_json STRING,
    processed_at TIMESTAMP,
    item_discount_amount DECIMAL(14,2),
    shipping_cost DECIMAL(14,2),
    sales_channel STRING
);

-- ==========================================
-- 5. ERROR HANDLING, ORPHANS & BUFFERS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_fk_orphans (
    orphan_id STRING,
    target_table STRING,
    source_record_id STRING,
    business_key STRING,
    event_id STRING,
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    store_id STRING,
    city_id STRING,
    fk_table STRING,
    fk_field STRING,
    fk_value STRING,
    orphan_reason STRING,
    is_resolved BOOLEAN,
    payload_json STRING,
    detected_at TIMESTAMP,
    item_discount_amount DECIMAL(14,2),
    sales_channel STRING,
    reprocessed BOOLEAN,
    late_reason STRING
);

CREATE TABLE IF NOT EXISTS ops.silver_late_arriving_buffer (
    buffer_id STRING,
    target_table STRING,
    child_table STRING,
    child_key STRING,
    event_id STRING,
    order_id STRING,
    payment_id STRING,
    refund_id STRING,
    return_id STRING,
    parent_table STRING,
    parent_key STRING,
    parent_key_value STRING,
    status STRING,
    retry_count INT,
    first_seen_at TIMESTAMP,
    last_retry_at TIMESTAMP,
    resolved_at TIMESTAMP,
    payload_json STRING,
    item_discount_amount DECIMAL(14,2),
    shipping_cost DECIMAL(14,2),
    sales_channel STRING,
    reason STRING
);

-- ==========================================
-- 6. REJECTIONS & QUARANTINE LOGS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.silver_rejected_records (
    rejection_id STRING,
    target_table STRING,
    file_name STRING,
    row_number INT,
    payment_id STRING,
    source_record_id STRING,
    event_id STRING,
    event_type STRING,
    order_id STRING,
    customer_id STRING,
    promotion_id STRING,
    rejection_stage STRING,
    rejection_reason STRING,
    failed_field STRING,
    failed_value STRING,
    payload_json STRING,
    bronze_checksum STRING,
    rejected_at TIMESTAMP,
    item_discount_amount DECIMAL(14,2),
    shipping_cost DECIMAL(14,2),
    sales_channel STRING,
    reason STRING,
    ticket_id STRING
);

CREATE TABLE IF NOT EXISTS ops.quarantine_inventory (
    quarantine_id STRING,
    inventory_id STRING,
    product_id STRING,
    store_id STRING,
    stock_quantity INT,
    snapshot_date_utc DATE,
    snapshot_timestamp_utc TIMESTAMP,
    bronze_file_name STRING,
    rejection_reason STRING,
    raw_payload STRING,
    rejected_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_rejection_log (
    log_id STRING,
    target_table STRING,
    rejection_reason STRING,
    rejected_count INT,
    bronze_file_name STRING,
    rejected_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.silver_to_gold_quarantine (
    quarantine_id STRING,
    target_table STRING,
    business_key STRING,
    order_id STRING,
    reason STRING,
    failed_field STRING,
    failed_value STRING,
    payload_json STRING,
    quarantined_at TIMESTAMP
);

-- ==========================================
-- 7. DATA QUALITY LOGS & EVENTS
-- ==========================================

CREATE TABLE IF NOT EXISTS ops.data_quality_log (
    id STRING,
    check_name STRING,
    table_name STRING,
    status STRING,
    details STRING,
    checked_at TIMESTAMP
);

-- (no DROP: preserve pipeline DQ event history across runs;
--  the CREATE IF NOT EXISTS below is a no-op when the table exists)
CREATE TABLE IF NOT EXISTS ops.silver_data_quality_events (
    id STRING,
    check_name STRING,
    target_table STRING,
    table_name STRING,
    business_key STRING,
    severity STRING,
    details STRING,
    details_json STRING,
    payload_json STRING,
    detected_at TIMESTAMP,
    event_id STRING,
    order_id STRING,
    issue_type STRING,
    description STRING,
    expected_value STRING,
    actual_value STRING,
    is_resolved BOOLEAN
);

-- ==========================================
-- 8. OPERATIONAL VIEWS
-- ==========================================

DROP VIEW IF EXISTS ops.fact_return;
DROP VIEW IF EXISTS ops.v_rejected_summary;

CREATE VIEW ops.fact_return AS 
SELECT 
    CAST(NULL AS STRING) AS order_id, 
    CAST(0 AS INT) AS is_returned, 
    CAST(NULL AS STRING) AS event_type;

CREATE VIEW ops.v_rejected_summary AS
SELECT 
    sales_channel, 
    rejection_reason, 
    COUNT(*) AS cnt, 
    MAX(rejected_at) AS last_at
FROM ops.silver_rejected_records 
GROUP BY sales_channel, rejection_reason;