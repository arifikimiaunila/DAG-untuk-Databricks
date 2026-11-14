"""Build Gold (Databricks Spark/Delta port) -- gold layer builder.

Ported from the original Airflow/psycopg2 implementation to run natively on
Databricks: builds the gold layer (workspace.gold) from the
silver tables (workspace.silver) as a full deterministic rebuild.
The Postgres INSERT ... ON CONFLICT upserts become CREATE OR REPLACE TABLE
... AS, which yields the same final state for a full silver refresh.
Postgres-specific syntax is translated as follows:
  DISTINCT ON ... ORDER BY  -> ROW_NUMBER() window, take _rn = 1
  AGG FILTER (WHERE ...)   -> SUM(CASE WHEN ... THEN ... ELSE 0 END)
  BOOL_OR(...)             -> MAX(CASE WHEN ... THEN 1 ELSE 0 END) = 1
  ::type casts             -> CAST(... AS type)
The original PostgreSQL/Airflow implementation is kept below, renamed
*_postgres_original_unused() with a leading raise (its unreachable body,
including the airflow/psycopg2 imports, is commented out so unresolved
imports are not flagged).
"""

import traceback
from datetime import UTC, datetime
from uuid import uuid4

from pyspark.sql import SparkSession

GOLD = "workspace.gold"
SILVER = "workspace.silver"
OPS = "workspace.ops"


def _ensure_gold_schema(spark_):
    """Create the gold schema and the ops.pipeline_runs metadata table."""
    spark_.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")
    spark_.sql(
        f"CREATE TABLE IF NOT EXISTS {OPS}.pipeline_runs ("
        "run_id STRING, dag_id STRING, status STRING, "
        "started_at TIMESTAMP, finished_at TIMESTAMP)"
    )


def _build_order_360(spark_, run_id):
    """Order-level 360 mart: sales, payments, refunds, returns per order."""
    spark_.sql(f"""
        CREATE OR REPLACE TABLE {GOLD}.order_360 AS
        WITH order_base AS (
            SELECT order_id, customer_id, order_date, total_amount,
                   shipping_revenue, sales_channel, store_id, order_status
            FROM (
                SELECT
                    o.order_id,
                    o.customer_id,
                    COALESCE(CAST(o.order_date AS DATE),
                             CAST(o.order_date_utc AS DATE),
                             CAST(o.ordered_at_utc AS DATE)) AS order_date,
                    COALESCE(o.amount, 0) AS total_amount,
                    COALESCE(o.shipping_revenue, 0) AS shipping_revenue,
                    COALESCE(NULLIF(o.sales_channel, ''), 'online') AS sales_channel,
                    o.store_id,
                    COALESCE(NULLIF(o.order_status, ''),
                             NULLIF(o.status, ''), 'unknown') AS order_status,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.order_id
                        ORDER BY o.updated_at_utc DESC NULLS LAST
                    ) AS _rn
                FROM {SILVER}.orders o
                WHERE o.order_id IS NOT NULL
                  AND o.customer_id IS NOT NULL
                  AND COALESCE(CAST(o.order_date AS DATE),
                               CAST(o.order_date_utc AS DATE),
                               CAST(o.ordered_at_utc AS DATE)) IS NOT NULL
                  AND COALESCE(o.is_current, TRUE)
            )
            WHERE _rn = 1
        ),
        item_totals AS (
            SELECT
                oi.order_id,
                SUM(COALESCE(oi.quantity, 0) * COALESCE(oi.unit_price, 0)) AS gmv,
                SUM(COALESCE(oi.item_discount_amount, oi.discount_amount, 0)) AS discount,
                CAST(COUNT(*) AS INT) AS item_count,
                CAST(SUM(COALESCE(oi.quantity, 0)) AS INT) AS unit_quantity
            FROM {SILVER}.order_items oi
            WHERE oi.order_id IS NOT NULL
              AND COALESCE(oi.is_orphan, FALSE) = FALSE
            GROUP BY oi.order_id
        ),
        order_promotion_totals AS (
            SELECT
                op.order_id,
                CAST(COUNT(DISTINCT op.promotion_id) AS INT) AS promotion_count,
                SUM(COALESCE(op.discount_amount, 0)) AS discount
            FROM {SILVER}.order_promotions op
            GROUP BY op.order_id
        ),
        payment_totals AS (
            SELECT
                order_id,
                SUM(CASE WHEN lower(COALESCE(payment_status, ''))
                              IN ('captured', 'paid', 'completed', 'success')
                         THEN COALESCE(amount, 0) ELSE 0 END) AS captured_amount,
                CASE
                    WHEN MAX(CASE WHEN lower(COALESCE(payment_status, ''))
                                       IN ('captured', 'paid', 'success')
                                  THEN 1 ELSE 0 END) = 1 THEN 'captured'
                    WHEN MAX(CASE WHEN lower(COALESCE(payment_status, '')) = 'completed'
                                  THEN 1 ELSE 0 END) = 1 THEN 'completed'
                    WHEN MAX(CASE WHEN lower(COALESCE(payment_status, '')) = 'refunded'
                                  THEN 1 ELSE 0 END) = 1 THEN 'refunded'
                    WHEN MAX(CASE WHEN lower(COALESCE(payment_status, '')) = 'pending'
                                  THEN 1 ELSE 0 END) = 1 THEN 'pending'
                    WHEN MAX(CASE WHEN lower(COALESCE(payment_status, '')) = 'failed'
                                  THEN 1 ELSE 0 END) = 1 THEN 'failed'
                    ELSE 'unknown'
                END AS payment_status
            FROM {SILVER}.payment_events
            GROUP BY order_id
        ),
        refund_totals AS (
            SELECT order_id, SUM(COALESCE(amount, 0)) AS refunded_amount
            FROM {SILVER}.refund_events
            GROUP BY order_id
        ),
        return_orders AS (
            SELECT order_id, event_type
            FROM (
                SELECT order_id, event_type,
                       ROW_NUMBER() OVER (PARTITION BY order_id
                           ORDER BY occurred_ts DESC NULLS LAST) AS _rn
                FROM {SILVER}.return_events
            )
            WHERE _rn = 1
        ),
        first_orders AS (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM order_base
            GROUP BY customer_id
        )
        SELECT
            o.order_id,
            o.customer_id,
            o.order_date,
            o.sales_channel,
            o.store_id,
            COALESCE(i.gmv, o.total_amount, 0) AS gross_merchandise_value,
            COALESCE(i.discount, 0) + COALESCE(p.discount, 0) AS discount_amount,
            COALESCE(o.shipping_revenue, 0) AS shipping_revenue,
            COALESCE(pay.captured_amount, o.total_amount, 0) AS captured_payment_amount,
            COALESCE(r.refunded_amount, 0) AS refunded_amount,
            ROUND(
                COALESCE(i.gmv, o.total_amount, 0)
                - (COALESCE(i.discount, 0) + COALESCE(p.discount, 0))
                + COALESCE(o.shipping_revenue, 0)
                - COALESCE(r.refunded_amount, 0), 2) AS net_revenue,
            COALESCE(i.item_count, 0) AS item_count,
            COALESCE(i.unit_quantity, 0) AS unit_quantity,
            o.order_status,
            COALESCE(pay.payment_status, 'unknown') AS payment_status,
            COALESCE(ro.event_type, 'NONE') AS return_status,
            COALESCE(p.promotion_count, 0) AS promotion_count,
            (o.order_date = f.first_order_date) AS first_order_flag,
            '{run_id}' AS pipeline_run_id
        FROM order_base o
        LEFT JOIN item_totals i ON i.order_id = o.order_id
        LEFT JOIN order_promotion_totals p ON p.order_id = o.order_id
        LEFT JOIN payment_totals pay ON pay.order_id = o.order_id
        LEFT JOIN refund_totals r ON r.order_id = o.order_id
        LEFT JOIN return_orders ro ON ro.order_id = o.order_id
        LEFT JOIN first_orders f ON f.customer_id = o.customer_id
    """)
    return spark_.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.order_360").first()["c"]


def _build_customer_daily(spark_, run_id):
    """Customer-per-day activity mart: orders, revenue, support, flags."""
    spark_.sql(f"""
        CREATE OR REPLACE TABLE {GOLD}.customer_daily AS
        WITH activity AS (
            SELECT customer_id, order_date AS metric_date,
                   CAST(COUNT(*) AS INT) AS order_count,
                   CAST(SUM(unit_quantity) AS INT) AS unit_quantity,
                   SUM(gross_merchandise_value) AS gross_revenue,
                   SUM(net_revenue) AS net_revenue,
                   SUM(refunded_amount) AS refund_amount,
                   CAST(SUM(CASE WHEN return_status <> 'NONE'
                                     AND return_status IS NOT NULL
                                THEN 1 ELSE 0 END) AS INT) AS return_count,
                   MAX(CASE WHEN first_order_flag THEN 1 ELSE 0 END) = 1
                       AS new_customer_flag
            FROM {GOLD}.order_360
            GROUP BY customer_id, order_date
        ),
        channel_rank AS (
            SELECT customer_id, order_date, sales_channel,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_id, order_date
                       ORDER BY COUNT(*) DESC, sales_channel
                   ) AS channel_rank
            FROM {GOLD}.order_360
            GROUP BY customer_id, order_date, sales_channel
        ),
        support_daily AS (
            SELECT COALESCE(t.customer_id, o.customer_id) AS customer_id,
                   COALESCE(CAST(t.occurred_ts AS DATE),
                            CAST(t.source_ingested_ts AS DATE),
                            CAST(t.ingested_at_utc AS DATE)) AS metric_date,
                   CAST(COUNT(DISTINCT t.ticket_id) AS INT) AS support_contacts
            FROM {SILVER}.support_events t
            LEFT JOIN {GOLD}.order_360 o ON o.order_id = t.order_id
            WHERE COALESCE(t.customer_id, o.customer_id) IS NOT NULL
              AND COALESCE(t.occurred_ts, t.source_ingested_ts) IS NOT NULL
            GROUP BY COALESCE(t.customer_id, o.customer_id),
                     COALESCE(CAST(t.occurred_ts AS DATE),
                              CAST(t.source_ingested_ts AS DATE),
                              CAST(t.ingested_at_utc AS DATE))
        ),
        customer_days AS (
            SELECT customer_id, metric_date FROM activity
            UNION
            SELECT customer_id, metric_date FROM support_daily
        ),
        customer_history AS (
            SELECT customer_id, MIN(order_date) AS first_order_date
            FROM {GOLD}.order_360
            GROUP BY customer_id
        )
        SELECT
            d.customer_id,
            d.metric_date,
            COALESCE(a.order_count, 0) AS order_count,
            COALESCE(a.unit_quantity, 0) AS unit_quantity,
            COALESCE(a.gross_revenue, 0) AS gross_revenue,
            COALESCE(a.net_revenue, 0) AS net_revenue,
            COALESCE(a.refund_amount, 0) AS refund_amount,
            COALESCE(a.return_count, 0) AS return_count,
            COALESCE(s.support_contacts, 0) AS support_contacts,
            c.sales_channel AS active_channel,
            COALESCE(a.new_customer_flag, FALSE) AS new_customer_flag,
            (h.first_order_date IS NOT NULL AND d.metric_date > h.first_order_date)
                AS repeat_customer_flag,
            '{run_id}' AS pipeline_run_id
        FROM customer_days d
        LEFT JOIN activity a
               ON a.customer_id = d.customer_id AND a.metric_date = d.metric_date
        LEFT JOIN channel_rank c
               ON c.customer_id = d.customer_id AND c.order_date = d.metric_date
              AND c.channel_rank = 1
        LEFT JOIN support_daily s
               ON s.customer_id = d.customer_id AND s.metric_date = d.metric_date
        LEFT JOIN customer_history h ON h.customer_id = d.customer_id
    """)
    return spark_.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.customer_daily").first()["c"]


def _build_product_daily(spark_, run_id):
    """Product-per-day mart: sales, inventory, stockout flag."""
    spark_.sql(f"""
        CREATE OR REPLACE TABLE {GOLD}.product_daily AS
        WITH product_sales AS (
            SELECT oi.product_id, o.order_date AS metric_date,
                   CAST(SUM(oi.quantity) AS INT) AS units_sold,
                   CAST(COUNT(DISTINCT oi.order_id) AS INT) AS order_count,
                   SUM(oi.quantity * oi.unit_price) AS gross_revenue,
                   SUM(COALESCE(oi.item_discount_amount, oi.discount_amount, 0)) AS discount,
                   CAST(COUNT(DISTINCT oi.promotion_id) AS INT) AS promotion_count
            FROM {SILVER}.order_items oi
            JOIN {GOLD}.order_360 o ON o.order_id = oi.order_id
            WHERE oi.product_id IS NOT NULL
            GROUP BY oi.product_id, o.order_date
        ),
        product_inventory AS (
            SELECT product_id, snapshot_date_utc AS metric_date,
                   CAST(SUM(stock_quantity) AS INT) AS available_inventory,
                   CAST(SUM(stock_reserved) AS INT) AS reserved_inventory
            FROM {SILVER}.inventory_snapshots
            WHERE product_id IS NOT NULL
              AND snapshot_date_utc IS NOT NULL
            GROUP BY product_id, snapshot_date_utc
        ),
        product_days AS (
            SELECT product_id, metric_date FROM product_sales
            UNION
            SELECT product_id, metric_date FROM product_inventory
        )
        SELECT
            d.product_id,
            d.metric_date,
            COALESCE(p.category_name, 'Uncategorized') AS active_category,
            COALESCE(s.units_sold, 0) AS units_sold,
            COALESCE(s.order_count, 0) AS order_count,
            COALESCE(s.gross_revenue, 0) AS gross_revenue,
            COALESCE(s.gross_revenue, 0) - COALESCE(s.discount, 0) AS net_revenue,
            0 AS refunded_units,
            i.available_inventory,
            i.reserved_inventory,
            CASE WHEN i.available_inventory IS NULL THEN NULL
                 ELSE i.available_inventory <= 0 END AS stockout_flag,
            COALESCE(s.promotion_count, 0) AS promotion_count,
            '{run_id}' AS pipeline_run_id
        FROM product_days d
        LEFT JOIN product_sales s
               ON s.product_id = d.product_id AND s.metric_date = d.metric_date
        LEFT JOIN product_inventory i
               ON i.product_id = d.product_id AND i.metric_date = d.metric_date
        LEFT JOIN {SILVER}.products p ON p.product_id = d.product_id
    """)
    return spark_.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.product_daily").first()["c"]


def _build_channel_campaign_daily(spark_, run_id):
    """Channel x campaign x day mart: spend, attribution, ROAS."""
    spark_.sql(f"""
        CREATE OR REPLACE TABLE {GOLD}.channel_campaign_daily AS
        WITH campaign_spend AS (
            SELECT cs.spend_date_utc AS metric_date,
                   COALESCE(sc.channel_name, 'UNKNOWN') AS sales_channel,
                   cs.campaign_id,
                   SUM(COALESCE(cs.spend_amount, 0)) AS campaign_spend
            FROM {SILVER}.campaign_spend cs
            LEFT JOIN {SILVER}.sales_channels sc ON sc.channel_id = cs.channel_id
            GROUP BY cs.spend_date_utc, COALESCE(sc.channel_name, 'UNKNOWN'), cs.campaign_id
        ),
        order_attribution AS (
            SELECT o.order_date AS metric_date,
                   o.sales_channel,
                   COALESCE(op.promotion_id, 'organic') AS campaign_id,
                   CAST(COUNT(DISTINCT o.order_id) AS INT) AS attributed_orders,
                   CAST(COUNT(DISTINCT o.customer_id) AS INT) AS attributed_customers,
                   SUM(COALESCE(o.gross_merchandise_value, 0)) AS gross_revenue,
                   SUM(COALESCE(o.net_revenue, 0)) AS net_revenue,
                   SUM(COALESCE(o.refunded_amount, 0)) AS refunds
            FROM {GOLD}.order_360 o
            LEFT JOIN {SILVER}.order_promotions op ON op.order_id = o.order_id
            GROUP BY o.order_date, o.sales_channel, COALESCE(op.promotion_id, 'organic')
        ),
        all_keys AS (
            SELECT metric_date, sales_channel, campaign_id FROM campaign_spend
            UNION
            SELECT metric_date, sales_channel, campaign_id FROM order_attribution
        )
        SELECT
            k.metric_date,
            k.sales_channel,
            k.campaign_id,
            COALESCE(s.campaign_spend, 0) AS campaign_spend,
            COALESCE(a.attributed_orders, 0) AS attributed_orders,
            COALESCE(a.attributed_customers, 0) AS attributed_customers,
            COALESCE(a.gross_revenue, 0) AS gross_revenue,
            COALESCE(a.net_revenue, 0) AS net_revenue,
            COALESCE(a.refunds, 0) AS refunds,
            CASE WHEN COALESCE(s.campaign_spend, 0) = 0 THEN NULL
                 ELSE COALESCE(a.net_revenue, 0) / s.campaign_spend END AS roas,
            NULL AS conversion_rate,
            '{run_id}' AS pipeline_run_id
        FROM all_keys k
        LEFT JOIN campaign_spend s
               ON s.metric_date = k.metric_date
              AND s.sales_channel = k.sales_channel
              AND s.campaign_id = k.campaign_id
        LEFT JOIN order_attribution a
               ON a.metric_date = k.metric_date
              AND a.sales_channel = k.sales_channel
              AND a.campaign_id = k.campaign_id
    """)
    return spark_.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.channel_campaign_daily").first()["c"]


def _build_executive_kpis(spark_, run_id):
    """Executive KPI mart: daily rates, active customers, data freshness."""
    spark_.sql(f"""
        CREATE OR REPLACE TABLE {GOLD}.executive_kpis_daily AS
        WITH order_daily AS (
            SELECT order_date AS metric_date,
                   CAST(COUNT(*) AS INT) AS total_orders,
                   SUM(gross_merchandise_value) AS gross_revenue,
                   SUM(net_revenue) AS net_revenue,
                   AVG(net_revenue) AS average_order_value,
                   CAST(SUM(CASE WHEN refunded_amount > 0 THEN 1 ELSE 0 END) AS DOUBLE)
                       / NULLIF(SUM(CASE WHEN captured_payment_amount > 0
                                         THEN 1 ELSE 0 END), 0)
                       AS refund_rate,
                   CAST(SUM(CASE WHEN return_status <> 'NONE' THEN 1 ELSE 0 END) AS DOUBLE)
                       / NULLIF(CAST(COUNT(*) AS DOUBLE), 0) AS return_rate,
                   CAST(COUNT(DISTINCT CASE WHEN first_order_flag = FALSE
                                            THEN customer_id END) AS DOUBLE)
                       / NULLIF(CAST(COUNT(DISTINCT customer_id) AS DOUBLE), 0)
                       AS repeat_customer_rate,
                   CAST(COUNT(DISTINCT customer_id) AS INT) AS active_customers
            FROM {GOLD}.order_360
            GROUP BY order_date
        ),
        product_daily AS (
            SELECT metric_date, AVG(CAST(stockout_flag AS INT)) AS stockout_rate
            FROM {GOLD}.product_daily
            WHERE stockout_flag IS NOT NULL
            GROUP BY metric_date
        ),
        support_daily AS (
            SELECT metric_date, SUM(support_contacts) AS support_contacts
            FROM {GOLD}.customer_daily
            GROUP BY metric_date
        ),
        freshness AS (
            SELECT MAX(ts) AS freshness_utc FROM (
                SELECT MAX(silver_processed_at) AS ts FROM {SILVER}.orders
                UNION ALL
                SELECT MAX(silver_processed_at) FROM {SILVER}.order_items
                UNION ALL
                SELECT MAX(silver_processed_at) FROM {SILVER}.payment_events
                UNION ALL
                SELECT MAX(silver_processed_at) FROM {SILVER}.refund_events
                UNION ALL
                SELECT MAX(silver_processed_at) FROM {SILVER}.inventory_snapshots
            )
        )
        SELECT
            o.metric_date,
            o.total_orders,
            COALESCE(o.gross_revenue, 0) AS gross_revenue,
            COALESCE(o.net_revenue, 0) AS net_revenue,
            COALESCE(o.average_order_value, 0) AS average_order_value,
            COALESCE(o.refund_rate, 0) AS refund_rate,
            COALESCE(o.return_rate, 0) AS return_rate,
            COALESCE(o.repeat_customer_rate, 0) AS repeat_customer_rate,
            COALESCE(p.stockout_rate, 0) AS stockout_rate,
            o.active_customers,
            COALESCE(s.support_contacts / NULLIF(o.total_orders, 0), 0)
                AS support_contact_rate,
            COALESCE(f.freshness_utc, current_timestamp()) AS data_freshness_utc,
            '{run_id}' AS pipeline_run_id
        FROM order_daily o
        LEFT JOIN product_daily p ON p.metric_date = o.metric_date
        LEFT JOIN support_daily s ON s.metric_date = o.metric_date
        CROSS JOIN freshness f
    """)
    return spark_.sql(f"SELECT COUNT(*) AS c FROM {GOLD}.executive_kpis_daily").first()["c"]


def validate_gold(spark_, run_id):
    """Reconcile gold.order_360 vs silver.orders and check net revenue."""
    row = spark_.sql(f"""
        SELECT
            (SELECT COUNT(*) FROM {GOLD}.order_360) AS gold_orders,
            (SELECT COUNT(*) FROM {SILVER}.orders
             WHERE order_id IS NOT NULL
               AND customer_id IS NOT NULL
               AND COALESCE(CAST(order_date AS DATE), CAST(order_date_utc AS DATE),
                            CAST(ordered_at_utc AS DATE)) IS NOT NULL
               AND COALESCE(is_current, TRUE)) AS eligible_orders,
            (SELECT COUNT(*) FROM {GOLD}.order_360
             WHERE ABS(ROUND(net_revenue, 2)
                     - ROUND(gross_merchandise_value - discount_amount
                             + shipping_revenue - refunded_amount, 2)) > 0.01)
                AS invalid
    """).first()
    print(f"[VALIDATE] gold={row['gold_orders']} "
          f"eligible={row['eligible_orders']} invalid={row['invalid']}")
    if row["gold_orders"] == 0:
        raise ValueError("Gold empty")
    if row["gold_orders"] != row["eligible_orders"]:
        raise ValueError(f"Count mismatch {row['gold_orders']}!={row['eligible_orders']}")
    if row["invalid"]:
        raise ValueError(f"Revenue fail {row['invalid']} rows")
    spark_.sql(
        f"INSERT INTO {OPS}.data_quality_log "
        "(check_name, table_name, status, details, checked_at) VALUES ("
        f"'gold_reconciliation', 'gold', 'PASS', "
        f"'run_id={run_id}; orders={row['gold_orders']}', current_timestamp())"
    )


def record_pipeline_metadata(spark_, run_id, status="SUCCESS"):
    """Append the run to ops.pipeline_runs."""
    spark_.sql(
        f"INSERT INTO {OPS}.pipeline_runs "
        f"VALUES ('{run_id}', 'omnichannel_retail_pipeline', '{status}', "
        "current_timestamp(), current_timestamp())"
    )


def run_gold(**context):
    """Build all gold tables, validate, and record run metadata."""
    dag_run = context.get("dag_run")
    run_id = (dag_run.run_id if dag_run
              else f"gold_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}")
    spark_ = SparkSession.builder.getOrCreate()
    try:
        _ensure_gold_schema(spark_)
        counts = {
            "order_360": _build_order_360(spark_, run_id),
            "customer_daily": _build_customer_daily(spark_, run_id),
            "product_daily": _build_product_daily(spark_, run_id),
            "channel_campaign_daily": _build_channel_campaign_daily(spark_, run_id),
            "executive_kpis_daily": _build_executive_kpis(spark_, run_id),
        }
        validate_gold(spark_, run_id)
        record_pipeline_metadata(spark_, run_id)
        result = {"status": "SUCCESS", "run_id": run_id, **counts}
        print(f"Gold refresh complete: {result}")
        return result
    except Exception as err:
        print(f"FAIL run_gold: {err}")
        traceback.print_exc()
        raise


def main(**context):
    return run_gold(**context)


if __name__ == "__main__":
    main()


# =========================================================================
# Original PostgreSQL/Airflow implementation (inactive, kept for reference)
# =========================================================================

def _get_conn_postgres_original_unused():
    """Original psycopg2/Airflow connection helper (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    # --- original body (unreachable; airflow/psycopg2 unavailable on Databricks) ---
    # try:
    #     from airflow.hooks.base import BaseHook
    #     c = BaseHook.get_connection('retail_postgres')
    #     return psycopg2.connect(host=c.host or 'postgres', port=c.port or 5432, dbname=c.schema or 'retail_analytics', user=c.login or 'retail', password=c.password or 'retail')
    # except Exception:
    #     return psycopg2.connect(host=os.getenv('POSTGRES_HOST','postgres'), port=int(os.getenv('POSTGRES_PORT','5432')), dbname=os.getenv('POSTGRES_DB','retail_analytics'), user=os.getenv('POSTGRES_USER','retail'), password=os.getenv('POSTGRES_PASSWORD','retail'))

def _build_order_360_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")

    cur.execute(
        """
        INSERT INTO gold.order_360 (

            order_id,
            customer_id,
            order_date,
            sales_channel,
            store_id,

            gross_merchandise_value,
            discount_amount,
            shipping_revenue,

            captured_payment_amount,
            refunded_amount,
            net_revenue,

            item_count,
            unit_quantity,

            order_status,
            payment_status,
            return_status,

            promotion_count,
            first_order_flag,

            pipeline_run_id
        )

        WITH order_base AS (

            SELECT DISTINCT ON (o.order_id)

                o.order_id,
                o.customer_id,

                COALESCE(
                    o.order_date,
                    o.order_date_utc::date,
                    o.ordered_at_utc::date
                ) AS order_date,

                COALESCE(
                    o.amount,
                    0
                ) AS total_amount,

                COALESCE(
                    o.shipping_revenue,
                    0
                ) AS shipping_revenue,

                COALESCE(
                    NULLIF(o.sales_channel,''),
                    'online'
                ) AS sales_channel,

                o.store_id,

                COALESCE(
                    NULLIF(o.order_status,''),
                    NULLIF(o.status,''),
                    'unknown'
                ) AS order_status

            FROM silver.orders o

            WHERE
                o.order_id IS NOT NULL
                AND o.customer_id IS NOT NULL
                AND COALESCE(
                    o.order_date,
                    o.order_date_utc::date,
                    o.ordered_at_utc::date
                ) IS NOT NULL
                AND COALESCE(o.is_current,TRUE)

            ORDER BY
                o.order_id,
                o.updated_at_utc DESC NULLS LAST
        ),

        item_totals AS (

            SELECT

                oi.order_id,

                SUM(
                    COALESCE(oi.quantity,0)
                    *
                    COALESCE(oi.unit_price,0)
                ) AS gmv,

                SUM(
                    COALESCE(
                        oi.item_discount_amount,
                        oi.discount_amount,
                        0
                    )
                ) AS discount,

                COUNT(*)::INTEGER
                    AS item_count,

                SUM(
                    COALESCE(
                        oi.quantity,
                        0
                    )
                )::INTEGER
                    AS unit_quantity

            FROM silver.order_items oi

            WHERE
                oi.order_id IS NOT NULL
                AND COALESCE(
                    oi.is_orphan,
                    FALSE
                ) = FALSE

            GROUP BY
                oi.order_id
        ),

        order_promotion_totals AS (

            SELECT

                op.order_id,

                COUNT(
                    DISTINCT op.promotion_id
                )::INTEGER
                    AS promotion_count,

                SUM(
                    COALESCE(
                        op.discount_amount,
                        0
                    )
                ) AS discount

            FROM silver.order_promotions op

            GROUP BY
                op.order_id
        ),

        payment_totals AS (

            SELECT

                order_id,

                SUM(
                    COALESCE(
                        amount,
                        0
                    )
                )

                FILTER (
                    WHERE lower(
                        COALESCE(
                            payment_status,
                            ''
                        )
                    )
                    IN (
                        'captured',
                        'paid',
                        'completed',
                        'success'
                    )
                )
                AS captured_amount,

                CASE

                    WHEN BOOL_OR(
                        lower(
                            COALESCE(
                                payment_status,
                                ''
                            )
                        )
                        IN (
                            'captured',
                            'paid',
                            'success'
                        )
                    )
                    THEN 'captured'

                    WHEN BOOL_OR(
                        lower(
                            COALESCE(
                                payment_status,
                                ''
                            )
                        )='completed'
                    )
                    THEN 'completed'

                    WHEN BOOL_OR(
                        lower(
                            COALESCE(
                                payment_status,
                                ''
                            )
                        )='refunded'
                    )
                    THEN 'refunded'

                    WHEN BOOL_OR(
                        lower(
                            COALESCE(
                                payment_status,
                                ''
                            )
                        )='pending'
                    )
                    THEN 'pending'

                    WHEN BOOL_OR(
                        lower(
                            COALESCE(
                                payment_status,
                                ''
                            )
                        )='failed'
                    )
                    THEN 'failed'

                    ELSE 'unknown'

                END AS payment_status

            FROM silver.payment_events

            GROUP BY
                order_id
        ),

        refund_totals AS (

            SELECT

                order_id,

                SUM(
                    COALESCE(
                        amount,
                        0
                    )
                ) AS refunded_amount

            FROM silver.refund_events

            GROUP BY
                order_id
        ),

        return_orders AS (

            SELECT DISTINCT ON (order_id)

                order_id,
                event_type

            FROM silver.return_events

            ORDER BY
                order_id,
                occurred_ts DESC NULLS LAST
        ),

        first_orders AS (

            SELECT

                customer_id,

                MIN(order_date)
                    AS first_order_date

            FROM order_base

            GROUP BY
                customer_id
        )

        SELECT

            o.order_id,
            o.customer_id,
            o.order_date,
            o.sales_channel,
            o.store_id,

            COALESCE(
                i.gmv,
                o.total_amount,
                0
            ),

            COALESCE(
                i.discount,
                0
            )
            +
            COALESCE(
                p.discount,
                0
            ),

            COALESCE(
                o.shipping_revenue,
                0
            ),

            COALESCE(
                pay.captured_amount,
                o.total_amount,
                0
            ),

            COALESCE(
                r.refunded_amount,
                0
            ),

            COALESCE(
                i.gmv,
                o.total_amount,
                0
            )

            -

            (
                COALESCE(
                    i.discount,
                    0
                )

                +

                COALESCE(
                    p.discount,
                    0
                )
            )

            +

            COALESCE(
                o.shipping_revenue,
                0
            )

            -

            COALESCE(
                r.refunded_amount,
                0
            ),

            COALESCE(
                i.item_count,
                0
            ),

            COALESCE(
                i.unit_quantity,
                0
            ),

            o.order_status,

            COALESCE(
                pay.payment_status,
                'unknown'
            ),

            COALESCE(
                ro.event_type,
                'NONE'
            ),

            COALESCE(
                p.promotion_count,
                0
            ),

            (
                o.order_date =
                f.first_order_date
            ),

            %s

        FROM order_base o

        LEFT JOIN item_totals i
            ON i.order_id = o.order_id

        LEFT JOIN order_promotion_totals p
            ON p.order_id = o.order_id

        LEFT JOIN payment_totals pay
            ON pay.order_id = o.order_id

        LEFT JOIN refund_totals r
            ON r.order_id = o.order_id

        LEFT JOIN return_orders ro
            ON ro.order_id = o.order_id

        LEFT JOIN first_orders f
            ON f.customer_id = o.customer_id

        ON CONFLICT (order_id)

        DO UPDATE SET

            customer_id =
                EXCLUDED.customer_id,

            order_date =
                EXCLUDED.order_date,

            sales_channel =
                EXCLUDED.sales_channel,

            store_id =
                EXCLUDED.store_id,

            gross_merchandise_value =
                EXCLUDED.gross_merchandise_value,

            discount_amount =
                EXCLUDED.discount_amount,

            shipping_revenue =
                EXCLUDED.shipping_revenue,

            captured_payment_amount =
                EXCLUDED.captured_payment_amount,

            refunded_amount =
                EXCLUDED.refunded_amount,

            net_revenue =
                EXCLUDED.net_revenue,

            item_count =
                EXCLUDED.item_count,

            unit_quantity =
                EXCLUDED.unit_quantity,

            order_status =
                EXCLUDED.order_status,

            payment_status =
                EXCLUDED.payment_status,

            return_status =
                EXCLUDED.return_status,

            promotion_count =
                EXCLUDED.promotion_count,

            first_order_flag =
                EXCLUDED.first_order_flag,

            pipeline_run_id =
                EXCLUDED.pipeline_run_id
        """,
        (run_id,)
    )

    return cur.rowcount

def _build_customer_daily_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    cur.execute("""
        INSERT INTO gold.customer_daily (customer_id, metric_date, order_count, unit_quantity, gross_revenue, net_revenue, refund_amount, return_count, support_contacts, active_channel, new_customer_flag, repeat_customer_flag, pipeline_run_id)
        WITH activity AS (
            SELECT customer_id, order_date AS metric_date, COUNT(*)::integer AS order_count, SUM(unit_quantity)::integer AS unit_quantity, SUM(gross_merchandise_value) AS gross_revenue, SUM(net_revenue) AS net_revenue, SUM(refunded_amount) AS refund_amount, COUNT(*) FILTER (WHERE return_status<>'NONE' AND return_status IS NOT NULL)::integer AS return_count, BOOL_OR(first_order_flag) AS new_customer_flag FROM gold.order_360 GROUP BY customer_id, order_date
        ),
        channel_rank AS (SELECT customer_id, order_date, sales_channel, ROW_NUMBER() OVER (PARTITION BY customer_id, order_date ORDER BY COUNT(*) DESC, sales_channel) AS channel_rank FROM gold.order_360 GROUP BY customer_id, order_date, sales_channel),
        support_daily AS (SELECT COALESCE(t.customer_id,o.customer_id) AS customer_id, COALESCE(t.occurred_ts::date, t.source_ingested_ts::date, t.ingested_at_utc::date) AS metric_date, COUNT(DISTINCT t.ticket_id)::integer AS support_contacts FROM silver.support_events t LEFT JOIN gold.order_360 o ON o.order_id=t.order_id WHERE COALESCE(t.customer_id,o.customer_id) IS NOT NULL AND COALESCE(t.occurred_ts, t.source_ingested_ts) IS NOT NULL GROUP BY 1,2),
        customer_days AS (SELECT customer_id, metric_date FROM activity UNION SELECT customer_id, metric_date FROM support_daily),
        customer_history AS (SELECT customer_id, MIN(order_date) AS first_order_date FROM gold.order_360 GROUP BY customer_id)
        SELECT d.customer_id, d.metric_date, COALESCE(a.order_count,0), COALESCE(a.unit_quantity,0), COALESCE(a.gross_revenue,0), COALESCE(a.net_revenue,0), COALESCE(a.refund_amount,0), COALESCE(a.return_count,0), COALESCE(s.support_contacts,0), c.sales_channel, COALESCE(a.new_customer_flag,FALSE), h.first_order_date IS NOT NULL AND d.metric_date>h.first_order_date, %s FROM customer_days d LEFT JOIN activity a ON a.customer_id=d.customer_id AND a.metric_date=d.metric_date LEFT JOIN channel_rank c ON c.customer_id=d.customer_id AND c.order_date=d.metric_date AND c.channel_rank=1 LEFT JOIN support_daily s ON s.customer_id=d.customer_id AND s.metric_date=d.metric_date LEFT JOIN customer_history h ON h.customer_id=d.customer_id ON CONFLICT (customer_id, metric_date) DO UPDATE SET order_count=EXCLUDED.order_count, pipeline_run_id=EXCLUDED.pipeline_run_id
        """, (run_id,))
    return cur.rowcount

def _build_product_daily_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    cur.execute("""
        INSERT INTO gold.product_daily (product_id, metric_date, active_category, units_sold, order_count, gross_revenue, net_revenue, refunded_units, available_inventory, reserved_inventory, stockout_flag, promotion_count, pipeline_run_id)
        WITH product_sales AS (SELECT oi.product_id, o.order_date AS metric_date, SUM(oi.quantity)::integer AS units_sold, COUNT(DISTINCT oi.order_id)::integer AS order_count, SUM(oi.quantity*oi.unit_price) AS gross_revenue, SUM(COALESCE(oi.item_discount_amount, oi.discount_amount,0)) AS discount, COUNT(DISTINCT oi.promotion_id)::integer AS promotion_count FROM silver.order_items oi JOIN gold.order_360 o ON o.order_id=oi.order_id WHERE oi.product_id IS NOT NULL GROUP BY oi.product_id, o.order_date),
        product_inventory AS (SELECT product_id, snapshot_date_utc AS metric_date, SUM(stock_quantity)::integer AS available_inventory, SUM(stock_reserved)::integer AS reserved_inventory FROM silver.inventory_snapshots WHERE product_id IS NOT NULL AND snapshot_date_utc IS NOT NULL GROUP BY product_id, snapshot_date_utc),
        product_days AS (SELECT product_id, metric_date FROM product_sales UNION SELECT product_id, metric_date FROM product_inventory)
        SELECT d.product_id, d.metric_date, COALESCE(p.category_name,'Uncategorized'), COALESCE(s.units_sold,0), COALESCE(s.order_count,0), COALESCE(s.gross_revenue,0), COALESCE(s.gross_revenue,0)-COALESCE(s.discount,0), 0, i.available_inventory, i.reserved_inventory, CASE WHEN i.available_inventory IS NULL THEN NULL ELSE i.available_inventory<=0 END, COALESCE(s.promotion_count,0), %s FROM product_days d LEFT JOIN product_sales s ON s.product_id=d.product_id AND s.metric_date=d.metric_date LEFT JOIN product_inventory i ON i.product_id=d.product_id AND i.metric_date=d.metric_date LEFT JOIN silver.products p ON p.product_id=d.product_id ON CONFLICT (product_id, metric_date) DO UPDATE SET units_sold=EXCLUDED.units_sold, pipeline_run_id=EXCLUDED.pipeline_run_id
        """, (run_id,))
    return cur.rowcount

def _build_channel_campaign_daily_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")

    cur.execute(
        """
        INSERT INTO gold.channel_campaign_daily (

            metric_date,
            sales_channel,
            campaign_id,

            campaign_spend,

            attributed_orders,
            attributed_customers,

            gross_revenue,
            net_revenue,
            refunds,

            roas,
            conversion_rate,

            pipeline_run_id
        )

        WITH campaign_spend AS (

            SELECT

                cs.spend_date_utc
                    AS metric_date,

                COALESCE(
                    sc.channel_name,
                    'UNKNOWN'
                ) AS sales_channel,

                cs.campaign_id,

                SUM(
                    COALESCE(
                        cs.spend_amount,
                        0
                    )
                ) AS campaign_spend

            FROM silver.campaign_spend cs

            LEFT JOIN silver.sales_channels sc
                ON sc.channel_id = cs.channel_id

            GROUP BY
                cs.spend_date_utc,
                COALESCE(sc.channel_name,'UNKNOWN'),
                cs.campaign_id
        ),

        order_attribution AS (

            SELECT

                o.order_date
                    AS metric_date,

                o.sales_channel,

                COALESCE(
                    op.promotion_id,
                    'organic'
                ) AS campaign_id,

                COUNT(
                    DISTINCT o.order_id
                )::INTEGER
                    AS attributed_orders,

                COUNT(
                    DISTINCT o.customer_id
                )::INTEGER
                    AS attributed_customers,

                SUM(
                    COALESCE(
                        o.gross_merchandise_value,
                        0
                    )
                ) AS gross_revenue,

                SUM(
                    COALESCE(
                        o.net_revenue,
                        0
                    )
                ) AS net_revenue,

                SUM(
                    COALESCE(
                        o.refunded_amount,
                        0
                    )
                ) AS refunds

            FROM gold.order_360 o

            LEFT JOIN silver.order_promotions op
                ON op.order_id = o.order_id

            GROUP BY
                o.order_date,
                o.sales_channel,
                COALESCE(
                    op.promotion_id,
                    'organic'
                )
        ),

        all_keys AS (

            SELECT
                metric_date,
                sales_channel,
                campaign_id
            FROM campaign_spend

            UNION

            SELECT
                metric_date,
                sales_channel,
                campaign_id
            FROM order_attribution
        )

        SELECT

            k.metric_date,

            k.sales_channel,

            k.campaign_id,

            COALESCE(
                s.campaign_spend,
                0
            ),

            COALESCE(
                a.attributed_orders,
                0
            ),

            COALESCE(
                a.attributed_customers,
                0
            ),

            COALESCE(
                a.gross_revenue,
                0
            ),

            COALESCE(
                a.net_revenue,
                0
            ),

            COALESCE(
                a.refunds,
                0
            ),

            CASE
                WHEN COALESCE(
                    s.campaign_spend,
                    0
                ) = 0
                THEN NULL

                ELSE
                    COALESCE(
                        a.net_revenue,
                        0
                    )
                    /
                    s.campaign_spend
            END,

            NULL,

            %s

        FROM all_keys k

        LEFT JOIN campaign_spend s
               ON s.metric_date = k.metric_date
              AND s.sales_channel = k.sales_channel
              AND s.campaign_id = k.campaign_id

        LEFT JOIN order_attribution a
               ON a.metric_date = k.metric_date
              AND a.sales_channel = k.sales_channel
              AND a.campaign_id = k.campaign_id

        ON CONFLICT (
            metric_date,
            sales_channel,
            campaign_id
        )

        DO UPDATE SET

            campaign_spend =
                EXCLUDED.campaign_spend,

            attributed_orders =
                EXCLUDED.attributed_orders,

            attributed_customers =
                EXCLUDED.attributed_customers,

            gross_revenue =
                EXCLUDED.gross_revenue,

            net_revenue =
                EXCLUDED.net_revenue,

            refunds =
                EXCLUDED.refunds,

            roas =
                EXCLUDED.roas,

            conversion_rate =
                EXCLUDED.conversion_rate,

            pipeline_run_id =
                EXCLUDED.pipeline_run_id
        """,
        (run_id,)
    )

    return cur.rowcount

def _build_executive_kpis_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")

    cur.execute(
        """
        INSERT INTO gold.executive_kpis_daily (

            metric_date,
            total_orders,

            gross_revenue,
            net_revenue,

            average_order_value,

            refund_rate,
            return_rate,
            repeat_customer_rate,
            stockout_rate,

            active_customers,

            support_contact_rate,

            data_freshness_utc,

            pipeline_run_id
        )

        WITH order_daily AS (

            SELECT

                order_date AS metric_date,

                COUNT(*)::INTEGER
                    AS total_orders,

                SUM(
                    gross_merchandise_value
                ) AS gross_revenue,

                SUM(
                    net_revenue
                ) AS net_revenue,

                AVG(
                    net_revenue
                ) AS average_order_value,

                COUNT(*)
                FILTER (
                    WHERE refunded_amount > 0
                )::NUMERIC

                /

                NULLIF(
                    COUNT(*)
                    FILTER (
                        WHERE captured_payment_amount > 0
                    ),
                    0
                )

                AS refund_rate,

                COUNT(*)
                FILTER (
                    WHERE return_status <> 'NONE'
                )::NUMERIC

                /

                NULLIF(
                    COUNT(*),
                    0
                )

                AS return_rate,

                COUNT(DISTINCT customer_id)
                FILTER (
                    WHERE first_order_flag = FALSE
                )::NUMERIC

                /

                NULLIF(
                    COUNT(DISTINCT customer_id),
                    0
                )

                AS repeat_customer_rate,

                COUNT(
                    DISTINCT customer_id
                )::INTEGER

                AS active_customers

            FROM gold.order_360

            GROUP BY
                order_date
        ),

        product_daily AS (

            SELECT

                metric_date,

                AVG(
                    stockout_flag::INTEGER
                )::NUMERIC

                AS stockout_rate

            FROM gold.product_daily

            WHERE stockout_flag IS NOT NULL

            GROUP BY
                metric_date
        ),

        support_daily AS (

            SELECT

                metric_date,

                SUM(
                    support_contacts
                )::NUMERIC

                AS support_contacts

            FROM gold.customer_daily

            GROUP BY
                metric_date
        ),

        freshness AS (

            SELECT

                GREATEST(

                    COALESCE(
                        (
                            SELECT MAX(
                                silver_processed_at
                            )
                            FROM silver.orders
                        ),
                        '-infinity'::timestamptz
                    ),

                    COALESCE(
                        (
                            SELECT MAX(
                                silver_processed_at
                            )
                            FROM silver.order_items
                        ),
                        '-infinity'::timestamptz
                    ),

                    COALESCE(
                        (
                            SELECT MAX(
                                silver_processed_at
                            )
                            FROM silver.payment_events
                        ),
                        '-infinity'::timestamptz
                    ),

                    COALESCE(
                        (
                            SELECT MAX(
                                silver_processed_at
                            )
                            FROM silver.refund_events
                        ),
                        '-infinity'::timestamptz
                    ),

                    COALESCE(
                        (
                            SELECT MAX(
                                silver_processed_at
                            )
                            FROM silver.inventory_snapshots
                        ),
                        '-infinity'::timestamptz
                    )

                ) AS freshness_utc
        )

        SELECT

            o.metric_date,

            o.total_orders,

            COALESCE(
                o.gross_revenue,
                0
            ),

            COALESCE(
                o.net_revenue,
                0
            ),

            COALESCE(
                o.average_order_value,
                0
            ),

            COALESCE(
                o.refund_rate,
                0
            ),

            COALESCE(
                o.return_rate,
                0
            ),

            COALESCE(
                o.repeat_customer_rate,
                0
            ),

            COALESCE(
                p.stockout_rate,
                0
            ),

            o.active_customers,

            COALESCE(
                s.support_contacts
                /
                NULLIF(
                    o.total_orders,
                    0
                ),
                0
            ),

            COALESCE(
                NULLIF(
                    f.freshness_utc,
                    '-infinity'::timestamptz
                ),
                NOW()
            ),

            %s

        FROM order_daily o

        LEFT JOIN product_daily p
            ON p.metric_date = o.metric_date

        LEFT JOIN support_daily s
            ON s.metric_date = o.metric_date

        CROSS JOIN freshness f

        ON CONFLICT (metric_date)

        DO UPDATE SET

            total_orders =
                EXCLUDED.total_orders,

            gross_revenue =
                EXCLUDED.gross_revenue,

            net_revenue =
                EXCLUDED.net_revenue,

            average_order_value =
                EXCLUDED.average_order_value,

            refund_rate =
                EXCLUDED.refund_rate,

            return_rate =
                EXCLUDED.return_rate,

            repeat_customer_rate =
                EXCLUDED.repeat_customer_rate,

            stockout_rate =
                EXCLUDED.stockout_rate,

            active_customers =
                EXCLUDED.active_customers,

            support_contact_rate =
                EXCLUDED.support_contact_rate,

            data_freshness_utc =
                EXCLUDED.data_freshness_utc,

            pipeline_run_id =
                EXCLUDED.pipeline_run_id

        """,
        (run_id,)
    )

    return cur.rowcount

def validate_gold_postgres_original_unused(cur, run_id):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    cur.execute("""SELECT COUNT(*), (SELECT COUNT(*) FROM silver.orders WHERE order_id IS NOT NULL AND customer_id IS NOT NULL AND COALESCE(order_date, order_date_utc::date, ordered_at_utc::date) IS NOT NULL AND COALESCE(is_current, TRUE)), (SELECT COUNT(*) FROM gold.order_360 WHERE ABS(net_revenue - (gross_merchandise_value - discount_amount + shipping_revenue - refunded_amount)) > 0.01) FROM gold.order_360""")
    gold_orders, eligible_orders, invalid = cur.fetchone()
    print(f"[VALIDATE] gold={gold_orders} eligible={eligible_orders} invalid={invalid}")
    if gold_orders==0: raise ValueError('Gold empty')
    if gold_orders!=eligible_orders: raise ValueError(f'Count mismatch {gold_orders}!={eligible_orders}')
    if invalid: raise ValueError(f'Revenue fail {invalid} rows')
    cur.execute("INSERT INTO ops.data_quality_log (check_name, table_name, status, details) VALUES (%s,%s,%s,%s)", ('gold_reconciliation','gold','PASS', f'run_id={run_id}; orders={gold_orders}'))

def record_pipeline_metadata_postgres_original_unused(cur, run_id, status='SUCCESS'):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    cur.execute("INSERT INTO ops.pipeline_runs (run_id, dag_id, status, started_at, finished_at) VALUES (%s,%s,%s,NOW(),NOW()) ON CONFLICT (run_id) DO UPDATE SET status=EXCLUDED.status, finished_at=EXCLUDED.finished_at", (run_id,'omnichannel_retail_pipeline',status))

def _run_gold_postgres_original_unused(**context):
    """Original psycopg2/Postgres implementation (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    dag_run=context.get('dag_run'); run_id=dag_run.run_id if dag_run else f"gold_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    conn = None  # get_conn() removed in the Databricks Spark port
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS gold')
                counts={'order_360': _build_order_360(cur, run_id), 'customer_daily': _build_customer_daily(cur, run_id), 'product_daily': _build_product_daily(cur, run_id), 'channel_campaign_daily': _build_channel_campaign_daily(cur, run_id), 'executive_kpis_daily': _build_executive_kpis(cur, run_id),}
                validate_gold(cur, run_id); record_pipeline_metadata(cur, run_id)
        result={'status':'SUCCESS','run_id':run_id, **counts}; print(f"Gold refresh complete: {result}"); return result
    except Exception as err:
        conn.rollback(); print(f"🚨 Error building Gold layer: {err}"); raise
    finally:
        conn.close()

def _main_postgres_original_unused(**context):
    """Original entry point (kept for reference only)."""
    raise RuntimeError("Postgres original disabled in Databricks port")
    return run_gold(**context)
