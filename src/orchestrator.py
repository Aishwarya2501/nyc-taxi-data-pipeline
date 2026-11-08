"""
NYC Taxi Pipeline Orchestrator

Architecture (Databricks Medallion):
- Bronze: Ingest raw Yellow/Green taxi Parquet & zone lookups.
- Silver: Align schemas, run validation rules, isolate quarantine, write clean Delta.
- Gold:   Enrich with dimensional zone names and publish to Unity Catalog.
"""

import os
from pyspark.sql import SparkSession

from ingest import load_taxi_data
from clean_transform import align_and_combine, add_zone_names
from quality_checks import run_quality_checks, summarize_quarantine

# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------
# Resolve project root dynamically (handles both terminal and notebook execution)
if "__file__" in globals():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
else:
    base_dir = os.path.dirname(os.getcwd())

PROJECT_ROOT = os.getenv("TAXI_DATA_DIR", base_dir)
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw_data")

EXPECTED_START = "2024-12-01"   
EXPECTED_END = "2025-02-01"     

# Unity Catalog target tables
CLEAN_TABLE = "workspace.default.taxi_trips"
QUARANTINE_TABLE = "workspace.default.taxi_quarantine"


def run_pipeline():
    spark = SparkSession.builder.appName("NYC_Taxi_Pipeline").getOrCreate()

    print("\n--- Starting NYC Taxi Data Pipeline ---")

    print("Step 1: Loading raw parquet files...")
    yellow_df, green_df, zones_df = load_taxi_data(spark, DATA_DIR)

    print("Step 2: Unifying schemas across Yellow and Green trips...")
    combined_df = align_and_combine(yellow_df, green_df)

    print("Step 3: Running data quality checks and filtering anomalies...")
    clean_df, quarantine_df = run_quality_checks(combined_df, EXPECTED_START, EXPECTED_END)

    passed_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    print(f"  • Passed trips: {passed_count:,}")
    print(f"  • Quarantined trips: {quarantine_count:,}")
    
    if quarantine_count > 0:
        print("\nQuarantine breakdown by failure rule:")
        summarize_quarantine(quarantine_df).show(truncate=False)

    print("\nStep 4: Enriching clean trips with zone names...")
    final_df = add_zone_names(clean_df, zones_df)

    print("Step 5: Writing datasets to Unity Catalog tables...")
    
    (
        final_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(CLEAN_TABLE)
    )

    (
        quarantine_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(QUARANTINE_TABLE)
    )

    print("\nPipeline completed successfully!")
    print(f"Ready for analysis: SELECT * FROM {CLEAN_TABLE}\n")


if __name__ == "__main__":
    run_pipeline()