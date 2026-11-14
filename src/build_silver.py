import os as _os
import sys as _sys
import traceback

try:
  _SRC_ROOT = _os.path.dirname(_os.path.abspath(__file__))
except NameError:
  _SRC_ROOT = "/Workspace/src/silver_files"
if _SRC_ROOT not in _sys.path:
  _sys.path.insert(0, _SRC_ROOT)

# Karena berada di satu folder, impor langsung tanpa 'silver_files.' sudah benar
from count_silver_rows import count_silver_rows
from stage1_lineage import stage1_lineage
from stage10_late import stage10_late
from stage11_quality_gate import stage11_quality_gate
from stage2_fill_silver import stage2_fill_silver
from stage3_timestamp import stage3_timestamp
from stage4_normalization import stage4_normalization
from stage5_rejection import stage5_rejection
from stage6_dedup import stage6_dedup
from stage7_identity import stage7_identity
from stage8_temporal import stage8_temporal
from stage9_fk import stage9_fk

def run_silver(**context):
  print("=== START SILVER PIPELINE (DATABRICKS UNIFIED) ===")
  try:
    stage1_lineage()
    stage2_fill_silver()
    count_silver_rows()
    stage3_timestamp()
    stage4_normalization()
    stage5_rejection()
    stage6_dedup()
    stage7_identity()
    stage8_temporal()
    stage9_fk()
    stage10_late()
    stage11_quality_gate()
    print("=== ALL SILVER PIPELINE STAGES COMPLETED SUCCESSFULLY ===")
    return "silver_full_ok"
  except Exception as e:
    print(f"🚨 Pipeline Silver Gagal pada eksekusi tahapan: {e}")
    traceback.print_exc()
    raise e

def build_silver_dag_callable(**context):
  return run_silver(**context)

def main(**context):
  return run_silver(**context)

if __name__ == "__main__":
  run_silver()