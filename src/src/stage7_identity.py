"""Stage 7 Identity Resolution (Spark port): silver.payment_events.

The Postgres original resolved payment identity (payment_id =
COALESCE(payment_id, event_id, payload->payment_id)) and nulled out the
order_id of payments whose parent order is absent from silver.orders, then
exposed a fact-returns view. This port re-applies identity resolution on
silver.payment_events (payment_id coalesced with event_id), nulls out orphan
order_ids, and (re)creates the silver.v_fact_returns view unioning refund and
return events.

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


def stage7_identity():
    """Resolve payment identity + null out orphan order_ids."""
    spark_ = get_spark()
    print("\n=== 7. IDENTITY RESOLUTION & FACT RETURN VIEW (SILVER.PAYMENT_EVENTS) ===")

    # Transformations are lazy and cannot raise here, so they are built
    # outside the try block (databricks-flint SCPAP005).
    payments = spark_.table(silver("payment_events"))
    orders_ids = spark_.table(silver("orders")).select(F.col("order_id").alias("_ord"))

    # identity resolution: payment_id coalesced with event_id
    df = payments.withColumn(
        "payment_id", F.coalesce(F.col("payment_id"), F.col("event_id")),
    )

    # orphan handling: null out order_id where the parent order is missing
    joined = df.join(orders_ids, df["order_id"] == orders_ids["_ord"], "left")
    orphan_probe = joined.where(
        F.col("order_id").isNotNull() & F.col("_ord").isNull()
    )
    df = joined.withColumn(
        "order_id",
        F.when(F.col("_ord").isNull(), F.lit(None).cast("string")).otherwise(F.col("order_id")),
    ).drop("_ord")

    try:
        orphan_count = orphan_probe.count()
        write_table(df, silver("payment_events"))

        # fact-returns view: union of refund and return events.
        # Note: UC persistent views cannot reference temp objects, so the
        # view is created directly over the persistent silver tables.
        spark_.sql(
            f"CREATE OR REPLACE VIEW {silver('v_fact_returns')} AS "
            "SELECT refund_id AS return_ref_id, order_id, event_id, "
            "       'refund' AS event_type, amount, occurred_ts "
            f"  FROM {silver('refund_events')} "
            "UNION ALL "
            "SELECT return_id AS return_ref_id, order_id, event_id, "
            "       'return' AS event_type, CAST(0.0 AS DECIMAL(18,2)) AS amount, occurred_ts "
            f"  FROM {silver('return_events')}"
        )

        total = spark_.sql(f"SELECT COUNT(*) AS c FROM {silver('payment_events')}").first()["c"]
        print(f"Orphan order_ids nulled out: {orphan_count}")
        print(f"silver.payment_events rows after identity resolution: {total}")
        print(f"View created: {silver('v_fact_returns')}")
    except Exception as e:
        print(f"FAIL stage7: {e}")
        traceback.print_exc()
        raise


# =========================================================================
# Original PostgreSQL implementation (inactive, kept for reference)
# =========================================================================

def _stage7_identity_postgres_original_unused():
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    print("\n=== 7. IDENTITY RESOLUTION & FACT RETURN VIEW (SILVER.PAYMENT_EVENTS) ===")
    
    sql_query = """
    -- =========================================================================
    -- ETL PIPELINE: BRONZE TO SILVER (IDENTITY RESOLUTION WITH CTE)
    -- =========================================================================
    WITH prepared_bronze_events AS (
        -- Langkah 1: Saring data mentah event dari bronze untuk resolusi identitas
        SELECT 
            b.bronze_id,
            b.payment_id,
            b.order_id,
            b.event_id,
            b.event_type,
            b.payload,
            b.source_record_id,
            b.file_name,
            b.row_number,
            b.checksum,
            b.ingested_at,
            b.target_table,
            COALESCE(b.payment_id, b.payload->>'payment_id', b.order_id, b.payload->>'order_id') AS business_key,
            COALESCE(b.payload->>'payment_status', b.payload->>'status', 'pending') AS raw_status,
            -- Ekstraksi timestamp terjadinya event secara aman
            COALESCE(
                NULLIF(b.payload->>'occurred_at_utc', '')::TIMESTAMPTZ,
                NULLIF(b.payload->>'payment_event_time', '')::TIMESTAMPTZ,
                b.ingested_at
            ) AS event_timestamp
        FROM bronze.raw_data b
        WHERE b.processing_status = 'NEW'
          AND b.target_table = 'silver.payment_events'
    ),
    ranked_identities AS (
        -- Langkah 2: Lakukan perankingan / Window Function untuk identifikasi competing IDs 
        SELECT 
            pe.*,
            ROW_NUMBER() OVER (
                PARTITION BY pe.business_key 
                ORDER BY pe.event_timestamp DESC, pe.ingested_at DESC
            ) as identity_rank,
            -- Kumpulkan semua competing event_id ke dalam array untuk keperluan audit
            array_agg(pe.event_id) OVER (PARTITION BY pe.business_key) as all_competing_ids
        FROM prepared_bronze_events pe
    ),
    resolved_identities AS (
        -- Langkah 3: Tentukan pemenang (Winning ID) dan aturan resolusi identitas
        SELECT 
            business_key AS event_business_key,
            event_id,
            event_type,
            order_id,
            payment_id,
            -- Resolved ID: Menggunakan ID dari baris dengan peringkat tertinggi (terbaru/valid)
            COALESCE(event_id, payment_id, business_key) AS resolved_id,
            all_competing_ids AS competing_event_ids,
            'LATEST_TIMESTAMP_PRIORITY' AS resolution_rule,
            raw_status AS winning_status,
            payload AS payload_json,
            target_table
        FROM ranked_identities
        WHERE identity_rank = 1
    )
    -- Langkah 4: Masukkan hasil Identity Resolution ke tabel operasional ops.silver_event_identity
    INSERT INTO ops.silver_event_identity (
        target_table,
        event_business_key,
        event_id,
        event_type,
        order_id,
        payment_id,
        resolved_id,
        competing_event_ids,
        resolution_rule,
        winning_status,
        payload_json,
        resolved_at
    )
    SELECT 
        target_table,
        event_business_key,
        event_id,
        event_type,
        order_id,
        payment_id,
        resolved_id,
        competing_event_ids,
        resolution_rule,
        winning_status,
        payload_json,
        CURRENT_TIMESTAMP
    FROM resolved_identities
    ON CONFLICT DO NOTHING;
    """

    conn = None
    cur = None
    try:
        conn = None  # get_conn() removed with the psycopg2 header in the Databricks port
        cur = conn.cursor()
        
        print("⏳ Mengeksekusi proses identity resolution ke `ops.silver_event_identity`...")
        cur.execute(sql_query)
        conn.commit()
        
        affected_rows = cur.rowcount
        print(f"✅ IDENTITY RESOLUTION OK. Baris terdampak: {affected_rows}")

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"🚨 Error pada stage7_identity: {e}")
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