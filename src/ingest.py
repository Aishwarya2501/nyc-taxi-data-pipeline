def load_taxi_data(spark,data_dir):
    # Read the parquet files
    yellow_df = spark.read.parquet(f"{data_dir}/yellow_tripdata_*.parquet")
    green_df = spark.read.parquet(f"{data_dir}/green_tripdata_*.parquet")
    # Read zone lookup file
    zones_df = spark.read.csv(f"{data_dir}/taxi_zone_lookup.csv", header=True, inferSchema=True)
    return yellow_df, green_df, zones_df