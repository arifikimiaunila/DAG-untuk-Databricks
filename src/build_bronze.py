import os
import pandas as pd

BASE_VOLUME_PATH = (
    "/Volumes/workspace/retail_nama_raw/omnicahnnel_retail_pipeline/data/raw"
)

DEFAULT_FILES = [
    "operational/customers.json",
    "operational/customer_addresses.json",
    "operational/customer_profiles.json",
    "operational/products.json",
    "operational/product_categories.json",
    "operational/stores.json",
    "operational/sales_channels.json",
    "operational/promotions.json",
    "operational/orders.json",
    "operational/order_items.json",
    "operational/order_promotions.json",
    "events/payment_events.json",
    "events/refund_events.json",
    "events/return_events.json",
    "events/support_events.json",
    "events/web_events.json",
    "inventory/inventory_snapshots.csv",
    "reference/campaign_spend.csv",
    "reference/city_reference.json",
]


def fill_raw_data():
  total_rows = 0

  for file in DEFAULT_FILES:
    full_path = f"{BASE_VOLUME_PATH}/{file}"
    base_file_name = os.path.basename(full_path)
    table_name = os.path.splitext(base_file_name)[0].lower()
    print(f">> Memproses file: {file} (Tabel Delta: {table_name})")

    try:
      # 1. Membaca file menggunakan Pandas
      if full_path.endswith(".csv"):
        pdf = pd.read_csv(full_path)
      elif full_path.endswith(".json"):
        # Mencoba membaca format JSON lines terlebih dahulu
        try:
          pdf = pd.read_json(full_path, lines=True)
        except ValueError:
          pdf = pd.read_json(full_path)
      else:
        continue

      row_count = len(pdf)
      if row_count == 0:
        print(f">> File {base_file_name} kosong, melewati...")
        continue

      # 2. Konversi Pandas DataFrame ke Spark DataFrame untuk disimpan ke Delta Table
      df_spark = spark.createDataFrame(pdf)

      target_table = f"workspace.bronze.{table_name}"
      df_spark.write.format("delta").mode("overwrite").saveAsTable(target_table)

      total_rows += row_count
      print(
          f">> Berhasil menyimpan {row_count} baris ke tabel Delta"
          f" {target_table}\n"
      )

    except Exception as e:
      print(f">> ERROR saat memproses {file}: {e}\n")

  return total_rows


def run_bronze():
  print(">> Memulai proses Bronze Layer...")
  total = fill_raw_data()
  print(f"\nDONE Bronze. Total baris diproses: {total}")
  return total


def build_bronze_dag_callable(**context):
  return run_bronze()


if __name__ == "__main__":
  run_bronze()