"""Silver row-count check (Spark port).

Original Airflow/Postgres version counted rows through a psycopg2 cursor;
this port counts the Delta silver tables in Unity Catalog. Missing or empty
tables raise the same alarm and (by default) fail the pipeline.
"""

import os as _os
import sys as _sys

# Bootstrap the import path so this file works standalone (Run button,
# `python count_silver_rows.py`, or a REPL) as well as via build_silver:
# ensure the sibling `common` module is importable in every context.
try:
    _HERE = _os.path.dirname(_os.path.abspath(__file__))
except NameError:  # executed without a file context (e.g. code pasted in a REPL)
    _HERE = "/Workspace/src/silver_files"
if _HERE not in _sys.path:
    _sys.path.insert(0, _HERE)

from common import get_spark, silver as silver_table_name

# Same tables and order as the Postgres original
SILVER_TABLES = [
    "customers",
    "customer_addresses",
    "customer_profiles",
    "city_reference",
    "products",
    "product_categories",
    "stores",
    "sales_channels",
    "orders",
    "order_items",
    "order_promotions",
    "payment_events",
    "refund_events",
    "return_events",
    "support_events",
    "web_events",
    "inventory_snapshots",
    "campaign_spend",
    "promotions",
]


def count_silver_rows(fail_on_empty=True):
    """Cek semua tabel silver. Jika ada tabel yang kosong (0 baris) atau NULL
    akibat kegagalan pengisian dari bronze, picu alarm keras dan gagalkan DAG.
    """
    spark = get_spark()
    failed_tables = []
    total_rows = 0

    print("\n" + "=" * 60)
    print("SILVER ROW COUNT CHECK + ALARM")
    print("=" * 60)

    for name in SILVER_TABLES:
        table = silver_table_name(name)
        try:
            count = spark.sql(f"SELECT COUNT(*) AS c FROM {table}").first()["c"]
            total_rows += count
            if count is None or count == 0:
                print(f"🚨 ALARM NULL/KOSONG | {name:35} | {count} rows -> GAGAL PENGISIAN!")
                failed_tables.append(table)
            else:
                print(f"✅ OK               | {name:35} | {count} rows")
        except Exception as e:
            print(f"🚨 ALARM MISSING      | {name:35} | ERROR: {e}")
            failed_tables.append(f"{table} (missing/error)")

    print("=" * 60)
    print(f"TOTAL SILVER ROWS: {total_rows}")
    print("=" * 60)

    if failed_tables:
        msg = (
            f"🚨🚨 ALARM: {len(failed_tables)} tabel silver KOSONG/NULL karena gagal "
            f"pengisian bronze: {', '.join(failed_tables)}"
        )
        print(msg)
        print("Saran: Cek kembali proses ingestion data mentah pada tabel bronze.")
        if fail_on_empty:
            raise ValueError(msg)
        return False

    print("✅ SEMUA TABEL SILVER TERISI - AMAN")
    print("=" * 60 + "\n")
    return True