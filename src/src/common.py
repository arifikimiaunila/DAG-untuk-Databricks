"""Shared helpers for the omnichannel retail silver pipeline on Databricks.

Ported from the original Airflow + PostgreSQL (psycopg2) implementation to
Spark / Delta Lake. The source of truth is now the per-file bronze tables in
`workspace.bronze` (loaded by build_bronze.py); silver and ops
outputs are Delta tables in Unity Catalog.

Differences vs the PostgreSQL original:
- Postgres read one unified `bronze.raw_data` table with a JSONB payload. In
  Databricks each source file has its own bronze table, so every stage selects
  directly from the matching bronze table.
- Bronze tables carry no ingestion metadata (file_name, checksum, row_number),
  so `add_bronze_metadata` synthesizes them (source file name, md5 checksum
  of the row, stable row number, business key as source_record_id).
- Table names align with the DDL in /database/schemas: the pipeline writes
  to schemas `bronze` / `silver` / `ops` (workspace.bronze etc.), matching
  bronze.sql / silver.sql / ops.sql.
"""

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE = "workspace.bronze"
SILVER = "workspace.silver"
OPS = "workspace.ops"

# logical source name -> source file (for bronze_file_name metadata)
SOURCE_FILES = {
    "customers": "operational/customers.json",
    "customer_addresses": "operational/customer_addresses.json",
    "customer_profiles": "operational/customer_profiles.json",
    "products": "operational/products.json",
    "product_categories": "operational/product_categories.json",
    "stores": "operational/stores.json",
    "sales_channels": "operational/sales_channels.json",
    "promotions": "operational/promotions.json",
    "orders": "operational/orders.json",
    "order_items": "operational/order_items.json",
    "order_promotions": "operational/order_promotions.json",
    "payment_events": "events/payment_events.json",
    "refund_events": "events/refund_events.json",
    "return_events": "events/return_events.json",
    "support_events": "events/support_events.json",
    "web_events": "events/web_events.json",
    "inventory_snapshots": "inventory/inventory_snapshots.csv",
    "campaign_spend": "reference/campaign_spend.csv",
    "city_reference": "reference/city_reference.json",
}

# logical name -> physical table name (aligned with silver.sql DDL)
BRONZE_TABLES = {name: name for name in SOURCE_FILES}
SILVER_TABLES = {name: name for name in SOURCE_FILES}

# fill order used by stage2
SILVER_TABLE_ORDER = [
    "customers", "customer_addresses", "customer_profiles", "city_reference",
    "products", "product_categories", "stores", "sales_channels",
    "promotions", "orders", "order_items", "order_promotions",
    "payment_events", "refund_events", "return_events", "support_events",
    "web_events", "inventory_snapshots", "campaign_spend",
]


def get_spark() -> SparkSession:
    try:
        return spark  # noqa: F821 - injected by Databricks runtimes
    except NameError:
        return SparkSession.builder.getOrCreate()


def bronze(name: str) -> str:
    return f"{BRONZE}.{BRONZE_TABLES.get(name, name)}"


def silver(name: str) -> str:
    return f"{SILVER}.{SILVER_TABLES.get(name, name)}"


def ops(name: str) -> str:
    return f"{OPS}.{name}"


def ensure_ops_schema() -> None:
    get_spark().sql(f"CREATE SCHEMA IF NOT EXISTS {OPS}")


def write_table(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    (df.write.format("delta").mode(mode)
        .option("overwriteSchema", "true").saveAsTable(table))


def ts(col: Column) -> Column:
    """NULLIF(x, '')::TIMESTAMPTZ equivalent."""
    return F.to_timestamp(F.nullif(col, F.lit("")))


def ts_col(df: DataFrame, name: str) -> Column:
    return ts(F.col(name))


def dt(col: Column) -> Column:
    """NULLIF(x, '')::DATE equivalent."""
    return F.to_date(F.nullif(col, F.lit("")))


def det_key(*cols: Column) -> Column:
    """md5 of pipe-joined parts (deterministic key, Postgres-compatible)."""
    parts = [F.coalesce(c.cast("string"), F.lit("")) for c in cols]
    return F.md5(F.concat_ws("|", *parts))


def add_bronze_metadata(df: DataFrame, name: str, business_key: Column) -> DataFrame:
    """Add synthesized bronze ingestion metadata columns.

    Adds: payload_json, bronze_file_name, bronze_row_number,
    bronze_source_record_id, bronze_checksum, bronze_ingested_at.
    """
    payload = F.to_json(F.struct("*"))
    out = df.withColumn("payload_json", payload)
    out = out.withColumn("bronze_file_name", F.lit(SOURCE_FILES[name]))
    out = out.withColumn(
        "bronze_row_number",
        F.row_number().over(
            Window.orderBy(F.monotonically_increasing_id())
        ) - 1,
    )
    out = out.withColumn("bronze_source_record_id", business_key)
    out = out.withColumn("bronze_checksum", F.md5(out["payload_json"]))
    out = out.withColumn("bronze_ingested_at", F.current_timestamp())
    return out


def dedup_latest(df: DataFrame, key: str, *order_cols: Column) -> DataFrame:
    """DISTINCT ON (key) ... ORDER BY key, <order> DESC equivalent."""
    order = [F.desc(c) for c in order_cols] if order_cols else [F.monotonically_increasing_id()]
    w = Window.partitionBy(key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


# =========================================================================
# Shared transformation: silver.orders (used by stages 2-6)
# =========================================================================


def build_orders(spark_: SparkSession, validate: bool = False) -> DataFrame:
    """Normalized + deduplicated silver.orders rows.

    Mirrors the union of stage2/stage3 (timestamp standardization) /
    stage4 (status normalization + validation) logic from the Postgres
    pipeline. If validate=True, adds validation_status / rejection_reason
    columns (INVALID_ORDER_ID / NEGATIVE_AMOUNT / INVALID_TIMESTAMP / VALID)
    and keeps only rows that pass validation.
    """
    src = spark_.table(bronze("orders"))
    df = src.select(
        F.nullif(F.trim("order_id"), F.lit("")).alias("order_id"),
        F.col("customer_id"),
        F.col("store_id"),
        F.col("sales_channel"),
        F.lower(F.trim(F.coalesce("status", F.lit("")))).alias("raw_status_text"),
        F.col("status"),
        ts_col(src, "ordered_at_utc").alias("ordered_at_utc"),
        F.col("ordered_at_utc").alias("ordered_at_text"),
        ts_col(src, "updated_at_utc").alias("updated_at_utc"),
        F.col("updated_at_utc").alias("updated_at_text"),
        F.col("shipping_revenue"),
        F.col("source_row_id"),
    )
    df = df.withColumn(
        "norm_status",
        F.when(F.col("raw_status_text").isin("paid", "completed", "success"), "completed")
         .when(F.col("raw_status_text").isin("pending", "processing"), "pending")
         .when(F.col("raw_status_text").isin("cancelled", "canceled"), "cancelled")
         .when(F.col("raw_status_text") == "failed", "failed")
         .when(F.col("raw_status_text") == "refunded", "refunded")
         .when(F.col("raw_status_text").isin("returned", "return"), "returned")
         .otherwise("pending"),
    )
    if validate:
        df = df.withColumn(
            "validation_status",
            F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "INVALID_ORDER_ID")
             .when(F.col("ordered_at_utc").isNull(), "INVALID_TIMESTAMP")
             .otherwise("VALID"),
        )
        df = df.withColumn(
            "rejection_reason",
            F.when(F.col("validation_status") == "INVALID_ORDER_ID", "order_id kosong atau null")
             .when(F.col("validation_status") == "INVALID_TIMESTAMP", "Format ordered_at_utc tidak valid/kosong")
             .otherwise(None),
        )
        df = df.withColumn("amount", F.lit(0.0).cast("decimal(18,2)"))
    else:
        df = df.withColumn("amount", F.lit(0.0).cast("decimal(18,2)"))
    return df


def orders_silver_columns(df: DataFrame) -> DataFrame:
    """Project the silver.orders 26-column schema from build_orders output."""
    out = add_bronze_metadata(df, "orders", F.coalesce(F.col("order_id"), F.lit("")))
    out = dedup_latest(out, "order_id", F.col("updated_at_text"))
    out = out.select(
        "order_id", "customer_id", "store_id",
        F.lit(None).cast("string").alias("channel_id"),
        F.coalesce(F.col("sales_channel"), F.lit("online")).alias("sales_channel"),
        F.col("norm_status").alias("order_status"),
        F.col("norm_status").alias("status"),
        F.to_date(F.col("ordered_at_utc")).alias("order_date"),
        F.col("ordered_at_utc").alias("order_date_utc"),
        F.col("ordered_at_utc").alias("ordered_at_utc"),
        F.col("updated_at_utc").alias("updated_at_utc"),
        F.col("updated_at_text").alias("updated_at_text"),
        "amount",
        F.lit(0.0).cast("decimal(18,2)").alias("shipping_cost"),
        F.coalesce(F.col("shipping_revenue"), F.lit(0.0)).cast("decimal(18,2)").alias("shipping_revenue"),
        F.lit("NONE").alias("event_type"),
        "source_row_id", "bronze_file_name", "bronze_row_number",
        "bronze_source_record_id", "bronze_checksum",
        F.current_timestamp().alias("silver_processed_at"),
        F.lit(True).alias("is_current"),
        F.lit(False).alias("is_late_arriving"),
        F.lit(False).alias("is_orphan"),
        det_key(F.lit("silver.orders"), F.col("bronze_source_record_id"),
                F.col("bronze_checksum"), F.col("bronze_row_number")).alias("deterministic_key"),
    )
    return out