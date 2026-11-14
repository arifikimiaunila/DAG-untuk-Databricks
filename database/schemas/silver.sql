-- ===========================================================================
-- Silver schema -- Databricks SQL port (ACTIVE)
-- ===========================================================================
-- Ported from the original PostgreSQL DDL below (kept as a commented
-- reference). Translation notes:
--   TEXT / NUMERIC(p,s) / TIMESTAMPTZ -> STRING / DECIMAL(p,s) / TIMESTAMP
--   DEFAULT NOW() -> DEFAULT current_timestamp() (Delta column defaults
--     need the allowColumnDefaults TBLPROPERTIES added to each table)
--   Unsupported Postgres constructs dropped: UNIQUE constraints (sku,
--     web_events.event_id), CHECK (price >= 0),
--     DEFAULT NOW() + INTERVAL '3 days' on order_items_pending (validation
--     for these lives in the pipeline stages, not in DDL).
-- The running pipeline (src/silver_files, src/build_silver.py) writes these
-- 19 tables via full rebuild; order_items_pending is unused by the port
-- (orphan buffering lives in workspace.ops.silver_fk_orphans instead).

CREATE SCHEMA IF NOT EXISTS workspace.silver;

CREATE TABLE IF NOT EXISTS workspace.silver.customers (
    customer_id VARCHAR(100) PRIMARY KEY,
    email VARCHAR(255),
    full_name VARCHAR(255),
    phone VARCHAR(50),
    city_id VARCHAR(100),
    customer_segment STRING,
    
    -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
    created_at_utc TIMESTAMP,
    valid_from_utc TIMESTAMP,
    valid_to_utc TIMESTAMP,
    valid_from_ts TIMESTAMP,
    valid_to_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & SCD Type 2 / State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT current_timestamp(),
    effective_to TIMESTAMP,
    version_number INT DEFAULT 1,
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.customer_addresses (
    address_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    city_id VARCHAR(100),
    
    -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
    valid_from_utc TIMESTAMP,
    valid_to_utc TIMESTAMP,
    valid_from_ts TIMESTAMP,
    valid_to_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & SCD Type 2 / State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    effective_from TIMESTAMP DEFAULT current_timestamp(),
    effective_to TIMESTAMP,
    version_number INT DEFAULT 1,
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- Kota
CREATE TABLE IF NOT EXISTS workspace.silver.city_reference (
    city_id VARCHAR(100) PRIMARY KEY,
    city_name STRING,
    country STRING,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.products (
    product_id VARCHAR(100) PRIMARY KEY,
    sku VARCHAR(100),
    product_name STRING,
    category_id VARCHAR(100),
    category_name STRING,
    price DECIMAL(14,2),
    unit_price DECIMAL(12,2),
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.product_categories (
    product_id VARCHAR(100),
    category_id VARCHAR(100),
    category_name STRING,
    
    -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
    valid_from_utc TIMESTAMP,
    valid_to_utc TIMESTAMP,
    valid_from_ts TIMESTAMP,
    valid_to_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512),
    
    -- Primary Key gabungan
    PRIMARY KEY (product_id, category_id)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.stores (
    store_id VARCHAR(100) PRIMARY KEY,
    store_name STRING,
    city_id VARCHAR(100),
    location_type STRING,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.sales_channels (
    channel_id VARCHAR(100) PRIMARY KEY,
    channel_name STRING,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.orders (
    order_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    store_id VARCHAR(100),
    channel_id VARCHAR(100),
    sales_channel STRING DEFAULT 'online',
    order_status STRING,
    status STRING,
    
    -- Kolom Waktu & Tanggal (diselaraskan dengan tipe data di Bronze)
    order_date DATE,
    order_date_utc TIMESTAMP,
    ordered_at_utc TIMESTAMP,
    updated_at_utc TIMESTAMP,
    updated_at_text STRING,
    
    -- Metrik Keuangan & Pengiriman
    amount DECIMAL(14,2) DEFAULT 100 NOT NULL,
    shipping_cost DECIMAL(14,2) DEFAULT 0,
    shipping_revenue DECIMAL(12,2) DEFAULT 0,
    event_type STRING DEFAULT 'NONE',
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    is_late_arriving BOOLEAN DEFAULT FALSE,
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.order_items (
    order_item_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    product_id VARCHAR(100),
    promotion_id VARCHAR(100),
    
    -- Kuantitas & Harga
    quantity DECIMAL(14,2),
    unit_price DECIMAL(12,2),
    item_discount_amount DECIMAL(14,2) DEFAULT 0,
    discount_amount INT DEFAULT 0,
    subtotal DECIMAL(14,2),
    sales_channel STRING DEFAULT 'online',
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_orphan BOOLEAN DEFAULT FALSE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.order_items_pending (
    order_item_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    
    -- Kuantitas & Harga
    quantity DECIMAL(14,2) NOT NULL,
    unit_price DECIMAL(12,2),
    item_discount_amount DECIMAL(14,2) DEFAULT 0,
    
    -- Status & Manajemen Orphan
    status STRING DEFAULT 'PENDING',
    is_orphan BOOLEAN DEFAULT TRUE,
    parent_search_deadline TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    ingested_at TIMESTAMP DEFAULT current_timestamp(),
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.promotions (
    promotion_id VARCHAR(100) PRIMARY KEY,
    promotion_code STRING,
    promotion_type STRING,
    
    -- Nilai Diskon & Periode
    discount_rate DECIMAL(14,2),
    discount_amount DECIMAL(14,2),
    start_date DATE,
    end_date DATE,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.order_promotions (
    order_id VARCHAR(100),
    promotion_id VARCHAR(100),
    
    -- Jumlah Diskon
    discount_amount INT DEFAULT 0,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512),
    
    -- Primary Key Gabungan
    PRIMARY KEY (order_id, promotion_id)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.campaign_spend (
    campaign_id VARCHAR(100),
    spend_date_utc DATE NOT NULL,
    channel_id VARCHAR(100),
    
    -- Metrik Pengeluaran
    spend_amount DECIMAL(14,2),
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512),
    snapshot_timestamp_utc TIMESTAMP,
    
    -- Primary Key Gabungan
    PRIMARY KEY (campaign_id, spend_date_utc)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.payment_events (
    payment_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    event_id STRING,
    event_type STRING,
    
    -- Metrik Keuangan Pembayaran
    amount DECIMAL(14,2),
    payment_status STRING,
    payment_method STRING,
    
    -- Kolom Waktu & Timestamp
    ingested_at_utc STRING,
    occurred_at_utc STRING,
    payment_event_time TIMESTAMP,
    occurred_ts TIMESTAMP,
    source_ingested_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.refund_events (
    refund_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    event_id STRING,
    event_type STRING,
    
    -- Metrik Keuangan Refund
    amount DECIMAL(14,2),
    
    -- Kolom Waktu & Timestamp
    ingested_at_utc STRING,
    occurred_at_utc STRING,
    occurred_ts TIMESTAMP,
    source_ingested_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.return_events (
    return_id VARCHAR(100) PRIMARY KEY,
    order_id VARCHAR(100),
    event_id STRING,
    event_type STRING,
    
    -- Kolom Waktu & Timestamp
    ingested_at_utc STRING,
    occurred_at_utc STRING,
    occurred_ts TIMESTAMP,
    source_ingested_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.support_events (
    ticket_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100),
    order_id VARCHAR(100),
    reason STRING,
    event_id STRING,
    event_type STRING,
    
    -- Kolom Waktu & Timestamp
    ingested_at_utc STRING,
    occurred_at_utc STRING,
    occurred_ts TIMESTAMP,
    source_ingested_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.web_events (
    event_id VARCHAR(100) PRIMARY KEY,
    session_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_id VARCHAR(100),
    campaign_id VARCHAR(100),
    channel STRING,
    sales_channel STRING,
    event_type STRING,
    page_url STRING,
    
    -- Kolom Waktu & Timestamp
    ingested_at_utc STRING,
    occurred_at_utc STRING,
    event_timestamp_utc TIMESTAMP,
    occurred_ts TIMESTAMP,
    source_ingested_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.inventory_snapshots (
    inventory_id VARCHAR(100),
    product_id VARCHAR(100),
    store_id VARCHAR(100),
    
    -- Metrik Inventaris & Stok
    stock_quantity INT,
    stock_on_hand INT,
    stock_reserved INT,
    
    -- Tanggal Snapshot
    snapshot_date DATE,
    snapshot_date_utc DATE,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512),
    snapshot_timestamp_utc TIMESTAMP,
    
    -- Primary Key Gabungan
    PRIMARY KEY (inventory_id, snapshot_date_utc)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

CREATE TABLE IF NOT EXISTS workspace.silver.customer_profiles (
    profile_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    city_id VARCHAR(100),
    customer_segment STRING,
    
    -- Kolom Tanggal & Efektif (SCD Type 2 / Effective Dates)
    valid_from_utc STRING,
    valid_to_utc STRING,
    valid_from_ts TIMESTAMP,
    valid_to_ts TIMESTAMP,
    
    -- Metadata Bronze & Lineage
    source_row_id STRING,
    source_record_id STRING,
    bronze_file_name VARCHAR(255),
    bronze_row_number INT,
    bronze_source_record_id VARCHAR(255),
    bronze_checksum VARCHAR(128),
    
    -- Metadata Silver & State Tracking
    silver_processed_at TIMESTAMP DEFAULT current_timestamp(),
    is_current BOOLEAN DEFAULT TRUE,
    deterministic_key VARCHAR(512)
)
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- (reference DDL converted from a block comment to line comments: the SQL
-- file task's statement splitter breaks on semicolons inside block comments)
-- CREATE SCHEMA IF NOT EXISTS silver;
--
-- CREATE TABLE IF NOT EXISTS silver.customers (
--     customer_id VARCHAR(100) PRIMARY KEY,
--     email VARCHAR(255),
--     full_name VARCHAR(255),
--     phone VARCHAR(50),
--     city_id VARCHAR(100),
--     customer_segment TEXT,
--
--     -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
--     created_at_utc TIMESTAMPTZ,
--     valid_from_utc TIMESTAMPTZ,
--     valid_to_utc TIMESTAMPTZ,
--     valid_from_ts TIMESTAMPTZ,
--     valid_to_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & SCD Type 2 / State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     effective_from TIMESTAMPTZ DEFAULT NOW(),
--     effective_to TIMESTAMPTZ,
--     version_number INT DEFAULT 1,
--     is_orphan BOOLEAN DEFAULT FALSE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.customer_addresses (
--     address_id VARCHAR(100) PRIMARY KEY,
--     customer_id VARCHAR(100),
--     city_id VARCHAR(100),
--
--     -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
--     valid_from_utc TIMESTAMPTZ,
--     valid_to_utc TIMESTAMPTZ,
--     valid_from_ts TIMESTAMPTZ,
--     valid_to_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & SCD Type 2 / State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     effective_from TIMESTAMPTZ DEFAULT NOW(),
--     effective_to TIMESTAMPTZ,
--     version_number INT DEFAULT 1,
--     is_orphan BOOLEAN DEFAULT FALSE,
--     deterministic_key VARCHAR(512)
-- );
--
-- -- Kota
-- CREATE TABLE IF NOT EXISTS silver.city_reference (
--     city_id VARCHAR(100) PRIMARY KEY,
--     city_name TEXT,
--     country TEXT,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.products (
--     product_id VARCHAR(100) PRIMARY KEY,
--     sku VARCHAR(100) UNIQUE,
--     product_name TEXT,
--     category_id VARCHAR(100),
--     category_name TEXT,
--     price NUMERIC(14,2) CHECK (price >= 0),
--     unit_price NUMERIC(12,2),
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.product_categories (
--     product_id VARCHAR(100),
--     category_id VARCHAR(100),
--     category_name TEXT,
--
--     -- Kolom Waktu & Validitas (diselaraskan dengan tipe data di Bronze)
--     valid_from_utc TIMESTAMPTZ,
--     valid_to_utc TIMESTAMPTZ,
--     valid_from_ts TIMESTAMPTZ,
--     valid_to_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512),
--
--     -- Primary Key gabungan
--     PRIMARY KEY (product_id, category_id)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.stores (
--     store_id VARCHAR(100) PRIMARY KEY,
--     store_name TEXT,
--     city_id VARCHAR(100),
--     location_type TEXT,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.sales_channels (
--     channel_id VARCHAR(100) PRIMARY KEY,
--     channel_name TEXT,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.orders (
--     order_id VARCHAR(100) PRIMARY KEY,
--     customer_id VARCHAR(100),
--     store_id VARCHAR(100),
--     channel_id VARCHAR(100),
--     sales_channel TEXT DEFAULT 'online',
--     order_status TEXT,
--     status TEXT,
--
--     -- Kolom Waktu & Tanggal (diselaraskan dengan tipe data di Bronze)
--     order_date DATE,
--     order_date_utc TIMESTAMPTZ,
--     ordered_at_utc TIMESTAMPTZ,
--     updated_at_utc TIMESTAMPTZ,
--     updated_at_text TEXT,
--
--     -- Metrik Keuangan & Pengiriman
--     amount NUMERIC(14,2) DEFAULT 100 NOT NULL,
--     shipping_cost NUMERIC(14,2) DEFAULT 0,
--     shipping_revenue NUMERIC(12,2) DEFAULT 0,
--     event_type TEXT DEFAULT 'NONE',
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     is_late_arriving BOOLEAN DEFAULT FALSE,
--     is_orphan BOOLEAN DEFAULT FALSE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.order_items (
--     order_item_id VARCHAR(100) PRIMARY KEY,
--     order_id VARCHAR(100),
--     product_id VARCHAR(100),
--     promotion_id VARCHAR(100),
--
--     -- Kuantitas & Harga
--     quantity NUMERIC(14,2),
--     unit_price NUMERIC(12,2),
--     item_discount_amount NUMERIC(14,2) DEFAULT 0,
--     discount_amount INT DEFAULT 0,
--     subtotal NUMERIC(14,2),
--     sales_channel TEXT DEFAULT 'online',
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_orphan BOOLEAN DEFAULT FALSE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.order_items_pending (
--     order_item_id VARCHAR(100) PRIMARY KEY,
--     order_id VARCHAR(100) NOT NULL,
--     product_id VARCHAR(100) NOT NULL,
--
--     -- Kuantitas & Harga
--     quantity NUMERIC(14,2) NOT NULL,
--     unit_price NUMERIC(12,2),
--     item_discount_amount NUMERIC(14,2) DEFAULT 0,
--
--     -- Status & Manajemen Orphan
--     status TEXT DEFAULT 'PENDING',
--     is_orphan BOOLEAN DEFAULT TRUE,
--     parent_search_deadline TIMESTAMPTZ DEFAULT NOW() + INTERVAL '3 days',
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     ingested_at TIMESTAMPTZ DEFAULT NOW(),
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.promotions (
--     promotion_id VARCHAR(100) PRIMARY KEY,
--     promotion_code TEXT,
--     promotion_type TEXT,
--
--     -- Nilai Diskon & Periode
--     discount_rate NUMERIC(14,2),
--     discount_amount NUMERIC(14,2),
--     start_date DATE,
--     end_date DATE,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.order_promotions (
--     order_id VARCHAR(100),
--     promotion_id VARCHAR(100),
--
--     -- Jumlah Diskon
--     discount_amount INT DEFAULT 0,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512),
--
--     -- Primary Key Gabungan
--     PRIMARY KEY (order_id, promotion_id)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.campaign_spend (
--     campaign_id VARCHAR(100),
--     spend_date_utc DATE NOT NULL,
--     channel_id VARCHAR(100),
--
--     -- Metrik Pengeluaran
--     spend_amount NUMERIC(14,2),
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512),
--     snapshot_timestamp_utc TIMESTAMPTZ,
--
--     -- Primary Key Gabungan
--     PRIMARY KEY (campaign_id, spend_date_utc)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.payment_events (
--     payment_id VARCHAR(100) PRIMARY KEY,
--     order_id VARCHAR(100),
--     event_id TEXT UNIQUE,
--     event_type TEXT,
--
--     -- Metrik Keuangan Pembayaran
--     amount NUMERIC(14,2),
--     payment_status TEXT,
--     payment_method TEXT,
--
--     -- Kolom Waktu & Timestamp
--     ingested_at_utc TEXT,
--     occurred_at_utc TEXT,
--     payment_event_time TIMESTAMPTZ,
--     occurred_ts TIMESTAMPTZ,
--     source_ingested_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.refund_events (
--     refund_id VARCHAR(100) PRIMARY KEY,
--     order_id VARCHAR(100),
--     event_id TEXT,
--     event_type TEXT,
--
--     -- Metrik Keuangan Refund
--     amount NUMERIC(14,2),
--
--     -- Kolom Waktu & Timestamp
--     ingested_at_utc TEXT,
--     occurred_at_utc TEXT,
--     occurred_ts TIMESTAMPTZ,
--     source_ingested_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.return_events (
--     return_id VARCHAR(100) PRIMARY KEY,
--     order_id VARCHAR(100),
--     event_id TEXT,
--     event_type TEXT,
--
--     -- Kolom Waktu & Timestamp
--     ingested_at_utc TEXT,
--     occurred_at_utc TEXT,
--     occurred_ts TIMESTAMPTZ,
--     source_ingested_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.support_events (
--     ticket_id VARCHAR(100) PRIMARY KEY,
--     customer_id VARCHAR(100),
--     order_id VARCHAR(100),
--     reason TEXT,
--     event_id TEXT,
--     event_type TEXT,
--
--     -- Kolom Waktu & Timestamp
--     ingested_at_utc TEXT,
--     occurred_at_utc TEXT,
--     occurred_ts TIMESTAMPTZ,
--     source_ingested_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.web_events (
--     event_id VARCHAR(100) PRIMARY KEY,
--     session_id VARCHAR(100),
--     customer_id VARCHAR(100),
--     order_id VARCHAR(100),
--     campaign_id VARCHAR(100),
--     channel TEXT,
--     sales_channel TEXT,
--     event_type TEXT,
--     page_url TEXT,
--
--     -- Kolom Waktu & Timestamp
--     ingested_at_utc TEXT,
--     occurred_at_utc TEXT,
--     event_timestamp_utc TIMESTAMPTZ,
--     occurred_ts TIMESTAMPTZ,
--     source_ingested_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
--     inventory_id VARCHAR(100),
--     product_id VARCHAR(100),
--     store_id VARCHAR(100),
--
--     -- Metrik Inventaris & Stok
--     stock_quantity INT,
--     stock_on_hand INT,
--     stock_reserved INT,
--
--     -- Tanggal Snapshot
--     snapshot_date DATE,
--     snapshot_date_utc DATE,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512),
--     snapshot_timestamp_utc TIMESTAMPTZ,
--
--     -- Primary Key Gabungan
--     PRIMARY KEY (inventory_id, snapshot_date_utc)
-- );
--
-- CREATE TABLE IF NOT EXISTS silver.customer_profiles (
--     profile_id VARCHAR(100) PRIMARY KEY,
--     customer_id VARCHAR(100) NOT NULL,
--     city_id VARCHAR(100),
--     customer_segment TEXT,
--
--     -- Kolom Tanggal & Efektif (SCD Type 2 / Effective Dates)
--     valid_from_utc TEXT,
--     valid_to_utc TEXT,
--     valid_from_ts TIMESTAMPTZ,
--     valid_to_ts TIMESTAMPTZ,
--
--     -- Metadata Bronze & Lineage
--     source_row_id TEXT,
--     source_record_id TEXT,
--     bronze_file_name VARCHAR(255),
--     bronze_row_number INT,
--     bronze_source_record_id VARCHAR(255),
--     bronze_checksum VARCHAR(128),
--
--     -- Metadata Silver & State Tracking
--     silver_processed_at TIMESTAMPTZ DEFAULT NOW(),
--     is_current BOOLEAN DEFAULT TRUE,
--     deterministic_key VARCHAR(512)
-- );
--
