"""Stage 1 Lineage (Spark port): build the unified ops.silver_lineage table.

The Postgres original read lineage rows out of a single `bronze.raw_data`
payload table. Here lineage is derived from the per-file bronze tables: every
table contributes the entity keys it carries, plus synthesized ingestion
metadata, and the union is written to `workspace.ops.silver_lineage`.
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

from common import (
    add_bronze_metadata,
    bronze,
    det_key,
    ensure_ops_schema,
    get_spark,
    ops,
)

# lineage entity columns contributed by each bronze table
LINEAGE_ENTITY_COLUMNS = [
    "order_id", "customer_id", "product_id", "payment_id", "event_id",
    "event_type", "city_id", "store_id", "channel_id", "address_id",
    "order_item_id", "promotion_id", "campaign_id", "refund_id",
    "return_id", "ticket_id", "amount", "shipping_revenue", "quantity",
    "order_status", "payment_status", "status",
]

# per-table overrides: bronze column (or None) for each entity column.
TABLE_ENTITY_MAP = {
    "customers": {"customer_id": "customer_id", "city_id": "city_id"},
    "customer_addresses": {
        "address_id": "address_id", "customer_id": "customer_id", "city_id": "city_id",
    },
    "customer_profiles": {"customer_id": "customer_id", "city_id": "city_id"},
    "city_reference": {"city_id": "city_id"},
    "products": {"product_id": "product_id"},
    "product_categories": {"product_id": "product_id"},
    "stores": {"store_id": "store_id", "city_id": "city_id"},
    "sales_channels": {"channel_id": "channel_id"},
    "promotions": {"promotion_id": "promotion_id"},
    "orders": {
        "order_id": "order_id", "customer_id": "customer_id", "store_id": "store_id",
        "shipping_revenue": "shipping_revenue", "status": "status",
    },
    "order_items": {
        "order_item_id": "order_item_id", "order_id": "order_id",
        "product_id": "product_id", "quantity": "quantity",
    },
    "order_promotions": {"order_id": "order_id", "promotion_id": "promotion_id"},
    "payment_events": {
        "payment_id": "payload.payment_id", "order_id": "payload.order_id",
        "event_id": "event_id", "event_type": "event_type",
        "amount": "payload.amount",
    },
    "refund_events": {
        "refund_id": "payload.refund_id", "order_id": "payload.order_id",
        "event_id": "event_id", "event_type": "event_type", "amount": "payload.amount",
    },
    "return_events": {
        "return_id": "payload.return_id", "order_id": "payload.order_id",
        "event_id": "event_id", "event_type": "event_type",
    },
    "support_events": {
        "ticket_id": "payload.ticket_id", "customer_id": "payload.customer_id",
        "order_id": "payload.order_id", "event_id": "event_id", "event_type": "event_type",
    },
    "web_events": {
        "event_id": "event_id", "event_type": "event_type",
        "campaign_id": "payload.campaign_id", "customer_id": "payload.customer_id",
        "order_id": "payload.order_id",
    },
    "inventory_snapshots": {
        "product_id": "product_id", "store_id": "location_id",
        "quantity": "available_quantity",
    },
    "campaign_spend": {"campaign_id": "campaign_id"},
}


def _lineage_for_table(spark_, name: str):
    entity_map = TABLE_ENTITY_MAP.get(name, {})
    # Read the schema via DESCRIBE TABLE (databricks-flint SCPAP001): accessing
    # DataFrame .columns inside a function that builds a new DataFrame fires an
    # Analyze RPC per call; DESCRIBE fetches the same catalog metadata in one
    # query. Rows starting with '#' (partition/clustering sections) are not
    # regular columns.
    described = spark_.sql(f"DESCRIBE TABLE {bronze(name)}").collect()
    src_cols = {r["col_name"] for r in described
                if r["col_name"] and not r["col_name"].startswith("#")}
    src = spark_.table(bronze(name))
    cols = {}
    for c in LINEAGE_ENTITY_COLUMNS:
        if c == "payment_status" and name == "payment_events":
            cols[c] = F.lit("SUCCESS")
            continue
        src_col = entity_map.get(c)
        if src_col is None:
            cols[c] = F.lit(None).cast("string")
        elif src_col in src_cols:
            cols[c] = F.col(src_col)
        else:
            cols[c] = F.lit(None).cast("string")
    # business key: first non-null entity id for the table
    key_candidates = [
        c for c in [
            "order_id", "order_item_id", "payment_id", "refund_id", "return_id",
            "ticket_id", "event_id", "customer_id", "address_id", "product_id",
            "store_id", "channel_id", "promotion_id", "campaign_id", "city_id",
        ]
        if entity_map.get(c)
    ]
    business_key = F.coalesce(*[cols[c] for c in key_candidates]) if key_candidates else F.lit(None)

    df = src.select(
        business_key.cast("string").alias("business_key"),
        *[cols[c].alias(c) for c in LINEAGE_ENTITY_COLUMNS],
    )
    df = add_bronze_metadata(df, name, F.coalesce(df["business_key"], F.lit("")))
    return df.select(
        F.monotonically_increasing_id().alias("lineage_id"),
        det_key(F.lit(f"silver.{name}"), F.col("bronze_source_record_id"),
                F.col("bronze_checksum"), F.col("bronze_row_number")).alias("deterministic_key"),
        "business_key",
        *[c for c in LINEAGE_ENTITY_COLUMNS],
        F.lit(None).cast("string").alias("source_table"),
        F.lit(f"silver.{name}").alias("target_table"),
        "bronze_file_name", "bronze_row_number", "bronze_source_record_id",
        "bronze_checksum", "bronze_ingested_at",
        F.col("bronze_file_name").alias("file_name"),
        "payload_json",
        F.lit("databricks_silver_pipeline").alias("job_name"),
        F.current_timestamp().alias("recorded_at"),
    )


def stage1_lineage():
    """Build the unified lineage table from all bronze tables."""
    spark_ = get_spark()
    print(">> STAGE1: Build ops.silver_lineage from bronze tables")

    # Transformations are lazy and cannot raise here, so the union is built
    # OUTSIDE the try block (databricks-flint SCPAP005): the try guards only
    # the actions that actually execute (schema DDL, the write, collect).
    dfs = [_lineage_for_table(spark_, name) for name in TABLE_ENTITY_MAP]
    unioned = dfs[0]
    for df in dfs[1:]:
        unioned = unioned.unionByName(df)

    try:
        ensure_ops_schema()
        # Triggering action for the lazy union above. Same semantics as
        # write_table(df, table) with mode="overwrite".
        (unioned.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(ops("silver_lineage")))

        counts = spark_.sql(
            f"SELECT target_table, COUNT(*) AS c FROM {ops('silver_lineage')}"
            " GROUP BY 1 ORDER BY 2 DESC"
        ).collect()
        print("=== FINAL LINEAGE BREAKDOWN ===")
        for row in counts:
            print(f" {row['c']:>6} | {row['target_table']}")
    except Exception as e:
        print(f"FAIL stage1: {e}")
        traceback.print_exc()
        raise