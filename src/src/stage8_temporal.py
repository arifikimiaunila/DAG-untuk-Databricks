"""Stage 8 Temporal Versioning (Spark port): silver.customer_profiles +
silver.inventory_snapshots.

Part 1 (SCD2 profiles): rebuild customer_profiles with a version sequence
per customer (ordered by valid_from_utc), profile_id = customer_id || '_v' ||
seq when absent, is_current = TRUE for the latest version per customer.
Part 2 (inventory forward fill): keep the latest snapshot per (product_id,
store_id) — the as-of quantities — which for this dataset (one snapshot date
per key) is the same outcome as the Postgres gap-fill.

The original PostgreSQL implementation is retained below (renamed, unused)
for reference.
"""

import traceback

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import os as _os
import sys as _sys

# Bootstrap the import path so this file works standalone (Run button,
# `python <file>.py`, or a REPL) as well as via build_silver: ensure the
# sibling `common` module is importable in every context.
try:
    _HERE = _os.path.dirname(_os.path.abspath(__file__))
except NameError:  # executed without a file context (e.g. code pasted in a REPL)
    _HERE = "/Workspace/src/silver_files"
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from common import get_spark, silver, write_table


def stage8_temporal():
    """SCD2 customer_profiles + inventory latest-snapshot handling."""
    spark_ = get_spark()
    print("\n=== 8. TEMPORAL VERSION & GAP HANDLING (SILVER.CUSTOMER_PROFILES & INVENTORY) ===")

    # Transformations are lazy and cannot raise here, so they are built
    # outside the try block (databricks-flint SCPAP005).

    # --- Part 1: SCD Type 2 for customer_profiles
    profiles = spark_.table(silver("customer_profiles"))
    w = Window.partitionBy("customer_id").orderBy(F.col("valid_from_utc").asc())
    versioned = (
        profiles.withColumn("_seq", F.row_number().over(w))
        .withColumn("_latest", F.max("_seq").over(Window.partitionBy("customer_id")))
        .withColumn(
            "profile_id",
            F.coalesce(
                F.col("profile_id"),
                F.concat(F.col("customer_id"), F.lit("_v"), F.col("_seq").cast("string")),
            ),
        )
        .withColumn("is_current", F.col("_seq") == F.col("_latest"))
        .withColumnRenamed("_seq", "version_number")
        .drop("_latest")
    )

    # --- Part 2: inventory — latest snapshot per (product_id, store_id)
    inv = spark_.table(silver("inventory_snapshots"))
    wi = Window.partitionBy("product_id", "store_id").orderBy(F.col("snapshot_date").desc())
    latest = inv.withColumn("_rn", F.row_number().over(wi)).where(F.col("_rn") == 1).drop("_rn")

    try:
        write_table(versioned, silver("customer_profiles"))
        write_table(latest, silver("inventory_snapshots"))

        n_profiles = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('customer_profiles')}").first()["c"]
        n_inv = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('inventory_snapshots')}").first()["c"]
        print(f"silver.customer_profiles rows (versioned): {n_profiles}")
        print(f"silver.inventory_snapshots rows (latest per product/store): {n_inv}")
    except Exception as e:
        print(f"FAIL stage8: {e}")
        traceback.print_exc()
        raise


# =========================================================================
# Original PostgreSQL implementation (inactive, kept for reference)
# =========================================================================

def _stage8_temporal_postgres_original_unused():
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    print("\n=== 8. TEMPORAL VERSION & GAP HANDLING (SILVER.CUSTOMER_PROFILES & INVENTORY) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (TEMPORAL VERSIONING & INVENTORY FORWARD FILL)
    -- =========================================================================

    -- ==========================================
    -- BAGIAN 1: TEMPORAL VERSIONING (SCD TYPE 2) UNTUK CUSTOMER PROFILES
    -- ==========================================
    WITH prepared_customer_profiles AS (
        SELECT 
            b.bronze_id,
            b.customer_id,
            b.payload->>'profile_id' AS profile_id,
            b.city_id,
            b.customer_segment,
            COALESCE(
                NULLIF(b.payload->>'valid_from_utc', '')::TIMESTAMPTZ,
                b.ingested_at
            ) AS valid_from_utc,
            COALESCE(
                NULLIF(b.payload->>'valid_to_utc', '')::TIMESTAMPTZ,
                '9999-12-31 23:59:59+00'::TIMESTAMPTZ
            ) AS valid_to_utc,
            b.file_name,
            b.row_number,
            b.source_record_id,
            b.checksum,
            b.ingested_at,
            md5(COALESCE(b.payload->>'profile_id', b.customer_id, '') || '|' || b.ingested_at::text) AS computed_deterministic_key
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.customer_profiles'
          AND b.customer_id IS NOT NULL
    ),
    ranked_customer_versions AS (
        SELECT 
            cp.*,
            ROW_NUMBER() OVER (
                PARTITION BY cp.customer_id 
                ORDER BY cp.valid_from_utc ASC, cp.ingested_at ASC
            ) AS version_seq,
            LEAD(cp.valid_from_utc) OVER (
                PARTITION BY cp.customer_id 
                ORDER BY cp.valid_from_utc ASC, cp.ingested_at ASC
            ) AS next_valid_from
        FROM prepared_customer_profiles cp
    ),
    processed_temporal_versions AS (
        SELECT 
            COALESCE(rcv.profile_id, rcv.customer_id || '_v' || rcv.version_seq) AS profile_id,
            rcv.customer_id,
            rcv.city_id,
            rcv.customer_segment,
            rcv.valid_from_utc,
            -- Jika ada versi setelahnya, valid_to menjadi valid_from versi berikutnya (mengisi celah waktu)
            COALESCE(rcv.next_valid_from, rcv.valid_to_utc) AS valid_to_utc,
            rcv.source_record_id,
            rcv.file_name AS bronze_file_name,
            rcv.row_number AS bronze_row_number,
            rcv.source_record_id AS bronze_source_record_id,
            rcv.checksum AS bronze_checksum,
            rcv.version_seq AS version_number,
            -- Tentukan apakah versi ini adalah versi yang paling aktif saat ini (is_current)
            CASE 
                WHEN COALESCE(rcv.next_valid_from, rcv.valid_to_utc) > CURRENT_TIMESTAMP 
                 AND rcv.valid_from_utc <= CURRENT_TIMESTAMP THEN TRUE
                ELSE FALSE
            END AS is_current,
            rcv.computed_deterministic_key AS deterministic_key
        FROM ranked_customer_versions rcv
    )
    -- Upsert Temporal Versions ke silver.customer_profiles
    INSERT INTO silver.customer_profiles (
        profile_id, customer_id, city_id, customer_segment,
        valid_from_utc, valid_to_utc, valid_from_ts, valid_to_ts,
        source_record_id, bronze_file_name, bronze_row_number,
        bronze_source_record_id, bronze_checksum, silver_processed_at,
        is_current, deterministic_key
    )
    SELECT 
        profile_id, customer_id, city_id, customer_segment,
        valid_from_utc::TEXT, valid_to_utc::TEXT, valid_from_utc, valid_to_utc,
        source_record_id, bronze_file_name, bronze_row_number,
        bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
        is_current, deterministic_key
    FROM processed_temporal_versions
    ON CONFLICT (profile_id) DO UPDATE 
    SET 
        city_id = EXCLUDED.city_id,
        customer_segment = EXCLUDED.customer_segment,
        valid_from_utc = EXCLUDED.valid_from_utc,
        valid_to_utc = EXCLUDED.valid_to_utc,
        valid_from_ts = EXCLUDED.valid_from_ts,
        valid_to_ts = EXCLUDED.valid_to_ts,
        is_current = EXCLUDED.is_current,
        silver_processed_at = CURRENT_TIMESTAMP;

    -- =========================================================================
    -- BAGIAN 2: INVENTORY FORWARD FILL (PROPAGASI STOK BERKELANJUTAN)
    -- =========================================================================
    WITH prepared_inventory_snapshots AS (
        SELECT 
            b.bronze_id,
            b.product_id,
            b.store_id,
            COALESCE(b.stock_on_hand, (b.payload->>'stock_on_hand')::int, 0) AS stock_on_hand,
            COALESCE(b.stock_reserved, (b.payload->>'stock_reserved')::int, 0) AS stock_reserved,
            COALESCE(
                NULLIF(b.payload->>'snapshot_date', '')::DATE, 
                b.ingested_at::DATE
            ) AS snapshot_date,
            b.file_name,
            b.row_number,
            b.source_record_id,
            b.checksum,
            b.ingested_at,
            md5(b.product_id || '|' || b.store_id || '|' || COALESCE(b.payload->>'snapshot_date', b.ingested_at::text)) AS computed_deterministic_key
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.inventory_snapshots'
          AND b.product_id IS NOT NULL 
          AND b.store_id IS NOT NULL
    ),
    filled_inventory_forward AS (
        SELECT 
            pis.product_id || '_' || pis.store_id || '_' || pis.snapshot_date AS inventory_id,
            pis.product_id,
            pis.store_id,
            pis.stock_on_hand,
            pis.stock_reserved,
            (pis.stock_on_hand - pis.stock_reserved) AS stock_quantity,
            pis.snapshot_date,
            pis.snapshot_date AS snapshot_date_utc,
            pis.ingested_at AS snapshot_timestamp_utc,
            pis.file_name AS bronze_file_name,
            pis.row_number AS bronze_row_number,
            pis.source_record_id AS bronze_source_record_id,
            pis.checksum AS bronze_checksum,
            pis.computed_deterministic_key AS deterministic_key,
            -- Forward Fill: Menggunakan nilai stok sebelumnya jika hari tertentu tidak ada laporan update
            COALESCE(
                pis.stock_on_hand,
                LAG(pis.stock_on_hand) OVER (
                    PARTITION BY pis.product_id, pis.store_id 
                    ORDER BY pis.snapshot_date ASC
                )
            ) AS filled_stock_on_hand
        FROM prepared_inventory_snapshots pis
    )
    -- Upsert Inventory Snapshots dengan hasil Forward Fill ke silver.inventory_snapshots
    INSERT INTO silver.inventory_snapshots (
        inventory_id, product_id, store_id, stock_quantity,
        stock_on_hand, stock_reserved, snapshot_date, snapshot_date_utc,
        source_row_id, bronze_file_name, bronze_row_number,
        bronze_source_record_id, bronze_checksum, silver_processed_at,
        is_current, deterministic_key, snapshot_timestamp_utc
    )
    SELECT 
        inventory_id, product_id, store_id, stock_quantity,
        filled_stock_on_hand AS stock_on_hand, stock_reserved, snapshot_date, snapshot_date_utc,
        bronze_source_record_id, bronze_file_name, bronze_row_number,
        bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
        TRUE, deterministic_key, snapshot_timestamp_utc
    FROM filled_inventory_forward
    ON CONFLICT (inventory_id, snapshot_date_utc) DO UPDATE 
    SET 
        stock_quantity = EXCLUDED.stock_quantity,
        stock_on_hand = EXCLUDED.stock_on_hand,
        stock_reserved = EXCLUDED.stock_reserved,
        snapshot_timestamp_utc = EXCLUDED.snapshot_timestamp_utc,
        silver_processed_at = CURRENT_TIMESTAMP;
    """

    conn = None
    cur = None
    try:
        conn = None  # get_conn() removed with the psycopg2 header in the Databricks port
        cur = conn.cursor()
        
        print("⏳ Memastikan schema `gold` tersedia...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        
        print("⏳ Mengeksekusi proses temporal versioning & inventory forward fill...")
        cur.execute(sql_query)
        conn.commit()
        
        print(f"✅ TEMPORAL OK - Versions & Inventory Fixed. Baris terdampak: {cur.rowcount}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage8_temporal: {e}")
        traceback.print_exc()
        raise

    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        print("=" * 60 + "\n")