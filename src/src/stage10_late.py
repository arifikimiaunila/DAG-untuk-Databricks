"""Stage 10 Late-Arriving Data (Spark port): silver.order_items_pending.

The Postgres original parked order_items whose parent order had not yet
arrived in a silver.order_items_pending table and moved pending rows back
once their parent order appeared. This port rebuilds that flow from the
current silver state: order_items whose order_id does not resolve in
silver.orders are parked in silver.order_items_pending (flagged
is_late_arriving), and any previously parked row whose parent order has
since arrived is moved back into silver.order_items.

The original PostgreSQL implementation is retained below (renamed, unused)
for reference.
"""

import traceback

from pyspark.sql import functions as F

from common import get_spark, silver, write_table


def stage10_late():
    """Park unresolved order_items; recover pending rows whose orders arrived."""
    spark_ = get_spark()
    print("\n=== 10. LATE ARRIVING & REPROCESSING (SILVER.ORDER_ITEMS_PENDING) ===")

    # Schema via DESCRIBE TABLE (databricks-flint SCPAP001): DataFrame.columns
    # on a DataFrame built in this function fires an Analyze RPC per call;
    # DESCRIBE reads the catalog metadata once. Column order is preserved
    # because DESCRIBE rows come in table order and select() follows the list.
    described = spark_.sql(f"DESCRIBE TABLE {silver('order_items')}").collect()
    items_cols = [r["col_name"] for r in described
                  if r["col_name"] and not r["col_name"].startswith("#")]

    # Transformations are lazy and cannot raise here, so they are built
    # outside the try block (databricks-flint SCPAP005).
    items = spark_.table(silver("order_items"))
    orders_ids = spark_.table(silver("orders")).select(F.col("order_id").alias("_ord"))

    # 1. split current items: resolvable vs pending (parent order missing)
    joined = items.join(orders_ids, items["order_id"] == orders_ids["_ord"], "left")
    resolvable = joined.where(F.col("_ord").isNotNull()).drop("_ord")
    pending_now = joined.where(F.col("_ord").isNull()).drop("_ord")

    # 2. previously parked rows whose parent orders have now arrived.
    # Note: spark_.table() on Spark Connect does not fail for a missing
    # table until the plan is analyzed, so a try/except around it never
    # fires here; check existence explicitly instead.
    recovered = None
    still_pending = None
    expected_pending_cols = set(items_cols) | {"is_late_arriving"}
    if spark_.catalog.tableExists(silver("order_items_pending")):
        described_p = spark_.sql(
            f"DESCRIBE TABLE {silver('order_items_pending')}").collect()
        pending_cols = {r["col_name"] for r in described_p
                        if r["col_name"] and not r["col_name"].startswith("#")}
        if pending_cols == expected_pending_cols:
            old_pending = spark_.table(silver("order_items_pending"))
            rec = old_pending.join(orders_ids,
                                   old_pending["order_id"] == orders_ids["_ord"], "left")
            recovered = rec.where(F.col("_ord").isNotNull()).drop("_ord")
            still_pending = rec.where(F.col("_ord").isNull()).drop("_ord")
        else:
            # The existing pending table has a different schema (e.g. the
            # empty DDL shell from silver.sql with the original Postgres
            # column set). Treat it as a fresh start instead of unioning
            # incompatible schemas; the write below replaces it with the
            # pipeline's own structure.
            print(f"pending table has unexpected schema "
                  f"({len(pending_cols)} cols, expected {len(expected_pending_cols)}) "
                  f"- treating as empty, rebuilding from scratch")

    # 3. rewrite the pending table: still-pending old rows + newly parked rows
    pending_all = pending_now.withColumn("is_late_arriving", F.lit(True))
    if still_pending is not None:
        pending_all = still_pending.withColumn("is_late_arriving", F.lit(True)).unionByName(pending_all)

    # 4. order_items keeps only resolvable rows (+ recovered ones). The
    # recovery probe runs BEFORE the pending table is overwritten so it
    # reads the previous run's parked rows, not the new ones.
    has_recovered = (
        recovered is not None
        and recovered.select("order_item_id").limit(1).count() > 0
    )
    final_items = resolvable.select(items_cols)
    # The explicit `recovered is not None` is implied by has_recovered but
    # lets the type checker narrow `None | DataFrame` to DataFrame here
    # (unresolved-attribute on recovered.select below otherwise).
    if has_recovered and recovered is not None:
        final_items = final_items.unionByName(recovered.select(items_cols))

    try:
        write_table(pending_all, silver("order_items_pending"))
        write_table(final_items, silver("order_items"))

        n_pend = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('order_items_pending')}").first()["c"]
        n_items = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('order_items')}").first()["c"]
        print(f"Parked in silver.order_items_pending (late-arriving): {n_pend}")
        print(f"silver.order_items rows after reprocessing: {n_items}")
    except Exception as e:
        print(f"FAIL stage10: {e}")
        traceback.print_exc()
        raise


# =========================================================================
# Original PostgreSQL implementation (inactive, kept for reference)
# =========================================================================

def _stage10_late_postgres_original_unused():
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    print("\n=== 10. LATE ARRIVING & REPROCESSING (SILVER.ORDER_ITEMS_PENDING) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (LATE-ARRIVING DATA HANDLING)
    -- =========================================================================
    WITH resolved_late_items AS (
        -- Langkah 1: Saring data dari tabel pending yang induknya (orders) sekarang sudah tersedia di silver
        SELECT 
            p.order_item_id,
            p.order_id,
            p.product_id,
            NULL::TEXT AS promotion_id,
            p.quantity,
            p.unit_price,
            p.item_discount_amount,
            (p.quantity * p.unit_price - p.item_discount_amount) AS subtotal,
            'online' AS sales_channel,
            p.source_row_id,
            p.bronze_file_name,
            p.bronze_row_number,
            p.bronze_source_record_id,
            p.bronze_checksum,
            p.deterministic_key
        FROM silver.order_items_pending p
        INNER JOIN silver.orders o ON p.order_id = o.order_id
        WHERE p.is_orphan = TRUE 
          AND p.status = 'PENDING_PARENT'
    ),
    insert_to_main_table AS (
        -- Langkah 2: Pindahkan data late-arriving yang sudah lengkap ke tabel utama `silver.order_items`
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
            sales_channel, source_row_id, bronze_file_name, bronze_row_number,
            bronze_source_record_id, bronze_checksum, CURRENT_TIMESTAMP,
            FALSE, deterministic_key
        FROM resolved_late_items
        ON CONFLICT (order_item_id) DO UPDATE 
        SET 
            order_id = EXCLUDED.order_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            item_discount_amount = EXCLUDED.item_discount_amount,
            subtotal = EXCLUDED.subtotal,
            silver_processed_at = CURRENT_TIMESTAMP,
            is_orphan = FALSE,
            deterministic_key = EXCLUDED.deterministic_key
        RETURNING order_item_id
    ),
    log_late_resolution AS (
        -- Langkah 3: Catat riwayat resolusi ke tabel buffer operasional `ops.silver_late_arriving_buffer`
        INSERT INTO ops.silver_late_arriving_buffer (
            target_table, child_table, child_key, order_id, 
            parent_table, parent_key, parent_key_value, status, 
            resolved_at, item_discount_amount, sales_channel, reason
        )
        SELECT 
            'silver.order_items', 'silver.order_items_pending', rli.order_item_id, rli.order_id,
            'silver.orders', 'order_id', rli.order_id, 'RESOLVED',
            CURRENT_TIMESTAMP, rli.item_discount_amount, rli.sales_channel, 
            'Parent order arrived late and successfully resolved'
        FROM resolved_late_items rli
        INNER JOIN insert_to_main_table im ON rli.order_item_id = im.order_item_id
        RETURNING buffer_id
    )
    -- Langkah 4: Hapus data yang telah berhasil diselesaikan dari tabel pending `silver.order_items_pending`
    DELETE FROM silver.order_items_pending p
    USING resolved_late_items rli
    WHERE p.order_item_id = rli.order_item_id;
    """

    conn = None
    cur = None
    try:
        conn = None  # get_conn() removed with the psycopg2 header in the Databricks port
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi proses resolusi data late-arriving...")
        cur.execute(sql_query)
        conn.commit()
        
        print("📊 Statistik Akhir Pipeline:")
        cur.execute("SELECT COUNT(*) FROM silver.payment_events;")
        print(f"   - Total baris `silver.payment_events`: {cur.fetchone()[0]}")
        
        cur.execute("SELECT COUNT(*) FROM silver.orders;")
        print(f"   - Total baris `silver.orders`: {cur.fetchone()[0]}")
        
        print("✅ LATE ARRIVING & REPROCESSING OK.")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage10_late: {e}")
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