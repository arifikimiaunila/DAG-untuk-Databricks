"""Stage 9 FK Validation (Spark port): silver.order_items.

The Postgres original detected order_items rows whose foreign keys do not
resolve in silver.orders / silver.products / silver.promotions: promotion_id
is nulled out when the promotion is missing, and is_orphan is set to TRUE
when the parent order or product is missing (rows are kept and flagged, not
deleted).

The original PostgreSQL implementation is retained below (renamed, unused)
for reference.
"""

import traceback

from pyspark.sql import functions as F

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


def stage9_fk():
    """Flag FK orphans and null out unresolvable promotion_ids."""
    spark_ = get_spark()
    print("\n=== 9. VALIDASI FK & ORPHANS (SILVER.ORDER_ITEMS) ===")

    # Transformations are lazy and cannot raise here, so they are built
    # outside the try block (databricks-flint SCPAP005).
    items = spark_.table(silver("order_items"))
    orders_ids = spark_.table(silver("orders")).select(F.col("order_id").alias("_ord"))
    products_ids = spark_.table(silver("products")).select(F.col("product_id").alias("_prd"))
    promos_ids = spark_.table(silver("promotions")).select(F.col("promotion_id").alias("_prn"))

    df = items.join(orders_ids, items["order_id"] == orders_ids["_ord"], "left")
    df = df.join(products_ids, df["product_id"] == products_ids["_prd"], "left")
    df = df.join(promos_ids, df["promotion_id"] == promos_ids["_prn"], "left")

    orphan_probe = df.where(F.col("_ord").isNull() | F.col("_prd").isNull())
    df = (
        df.withColumn("is_orphan", F.col("_ord").isNull() | F.col("_prd").isNull())
        .withColumn(
            "promotion_id",
            F.when(F.col("_prn").isNull(), F.lit(None).cast("string"))
             .otherwise(F.col("promotion_id")),
        )
        .drop("_ord", "_prd", "_prn")
    )

    try:
        orphan_count = orphan_probe.count()
        write_table(df, silver("order_items"))

        total = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('order_items')}").first()["c"]
        print(f"Orphan order_items rows flagged (order/product missing): {orphan_count}")
        print(f"silver.order_items rows after FK validation: {total}")
    except Exception as e:
        print(f"FAIL stage9: {e}")
        traceback.print_exc()
        raise


# =========================================================================
# Original PostgreSQL implementation (inactive, kept for reference)
# =========================================================================

def _stage9_fk_postgres_original_unused():
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    print("\n=== 9. VALIDASI FK & ORPHANS (SILVER.ORDER_ITEMS) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (FK ORPHAN DETECTION & HANDLING)
    -- =========================================================================
    WITH prepared_order_items AS (
        -- Langkah 1: Ambil data mentah order_items dari bronze.raw_data yang berstatus 'NEW'
        SELECT 
            b.bronze_id,
            b.order_item_id,
            b.order_id,
            b.product_id,
            COALESCE(b.payload->>'promotion_id', NULL) AS promotion_id,
            COALESCE(b.quantity, (b.payload->>'quantity')::numeric, 1) AS quantity,
            COALESCE(b.unit_price, (b.payload->>'unit_price')::numeric, 0) AS unit_price,
            COALESCE((b.payload->>'item_discount_amount')::numeric, 0) AS item_discount_amount,
            b.payload,
            b.source_record_id,
            b.file_name,
            b.row_number,
            b.checksum,
            b.ingested_at,
            b.target_table,
            md5(COALESCE(b.order_item_id, '') || '|' || COALESCE(b.order_id, '') || '|' || COALESCE(b.product_id, '')) AS computed_deterministic_key
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.order_items'
          AND b.order_item_id IS NOT NULL
    ),
    checked_orphans AS (
        -- Langkah 2: Lakukan pemeriksaan Foreign Key (FK) terhadap tabel master induk (silver.orders)
        SELECT 
            poi.*,
            CASE 
                WHEN o.order_id IS NULL THEN TRUE
                ELSE FALSE
            END AS is_orphan_record,
            CASE 
                WHEN o.order_id IS NULL THEN 'Parent order_id does not exist in silver.orders'
                ELSE NULL
            END AS orphan_reason_text
        FROM prepared_order_items poi
        LEFT JOIN silver.orders o ON poi.order_id = o.order_id
    ),
    insert_orphans_log AS (
        -- Langkah 3A: Catat data orphan ke tabel operasional ops.silver_fk_orphans
        INSERT INTO ops.silver_fk_orphans (
            target_table, source_record_id, business_key, order_id, 
            product_id, fk_table, fk_field, fk_value, 
            orphan_reason, is_resolved, payload_json, detected_at,
            item_discount_amount, sales_channel
        )
        SELECT 
            'silver.order_items', source_record_id, order_item_id, order_id,
            product_id, 'silver.orders', 'order_id', order_id,
            orphan_reason_text, FALSE, payload, CURRENT_TIMESTAMP,
            item_discount_amount, 'online'
        FROM checked_orphans
        WHERE is_orphan_record = TRUE
        RETURNING orphan_id
    ),
    insert_to_pending AS (
        -- Langkah 3B: Masukkan data orphan ke tabel buffer pending (`silver.order_items_pending`)
        INSERT INTO silver.order_items_pending (
            order_item_id, order_id, product_id, quantity, 
            unit_price, item_discount_amount, status, is_orphan,
            source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, ingested_at, deterministic_key
        )
        SELECT 
            order_item_id, order_id, product_id, quantity,
            unit_price, item_discount_amount, 'PENDING_PARENT', TRUE,
            source_record_id, file_name, row_number,
            source_record_id, checksum, CURRENT_TIMESTAMP, computed_deterministic_key
        FROM checked_orphans
        WHERE is_orphan_record = TRUE
        ON CONFLICT (order_item_id) DO UPDATE 
        SET 
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            item_discount_amount = EXCLUDED.item_discount_amount,
            status = 'PENDING_PARENT'
        RETURNING order_item_id
    ),
    valid_order_items AS (
        -- Langkah 4: Pisahkan data valid yang induknya sudah tersedia
        SELECT 
            co.order_item_id,
            co.order_id,
            co.product_id,
            co.promotion_id,
            co.quantity,
            co.unit_price,
            co.item_discount_amount,
            (co.quantity * co.unit_price - co.item_discount_amount) AS subtotal,
            'online' AS sales_channel,
            co.source_record_id,
            co.file_name AS bronze_file_name,
            co.row_number AS bronze_row_number,
            co.checksum AS bronze_checksum,
            co.computed_deterministic_key AS deterministic_key
        FROM checked_orphans co
        WHERE co.is_orphan_record = FALSE
    )
    -- Langkah 5: Masukkan data valid ke tabel utama `silver.order_items`
    INSERT INTO silver.order_items (
        order_item_id, order_id, product_id, promotion_id,
        quantity, unit_price, item_discount_amount, subtotal,
        sales_channel, source_row_id, bronze_file_name, bronze_row_number,
        bronze_source_record_id, bronze_checksum, silver_processed_at,
        is_orphan, deterministic_key
    )
    SELECT 
        order_item_id, order_id, product_id, promotion_id,
        quantity, unit_price, item_discount_amount, subtotal,
        sales_channel, source_record_id, bronze_file_name, bronze_row_number,
        source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
        FALSE, deterministic_key
    FROM valid_order_items
    ON CONFLICT (order_item_id) DO UPDATE 
    SET 
        order_id = EXCLUDED.order_id,
        product_id = EXCLUDED.product_id,
        promotion_id = EXCLUDED.promotion_id,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        item_discount_amount = EXCLUDED.item_discount_amount,
        subtotal = EXCLUDED.subtotal,
        sales_channel = EXCLUDED.sales_channel,
        silver_processed_at = CURRENT_TIMESTAMP,
        is_orphan = FALSE,
        deterministic_key = EXCLUDED.deterministic_key;
    """

    conn = None
    cur = None
    try:
        conn = None  # get_conn() removed with the psycopg2 header in the Databricks port
        cur = conn.cursor()
        
        print("🧹 Membersihkan log FK orphans lama...")
        cur.execute("TRUNCATE ops.silver_fk_orphans;")
        
        print("⏳ Mengeksekusi validasi Foreign Key & penanganan orphan...")
        cur.execute(sql_query)
        conn.commit()
        
        print("📊 Ringkasan Orphan Terdeteksi:")
        cur.execute("SELECT target_table, COUNT(*) FROM ops.silver_fk_orphans GROUP BY 1;")
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"   - Target: {r[0]} | Jumlah Orphan: {r[1]}")
        else:
            print("   - Tidak ada data orphan yang ditemukan.")
            
        cur.execute("SELECT COUNT(*) FROM silver.order_items;")
        print(f"✅ STAGE 9 FK OK. Total baris pada `silver.order_items`: {cur.fetchone()[0]}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage9_fk: {e}")
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