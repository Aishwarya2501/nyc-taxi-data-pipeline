from pyspark.sql import functions as F

def align_and_combine(yellow_df, green_df):

    yellow = yellow_df.withColumn("cab_colour", F.lit("Yellow"))
    green = green_df.withColumn("cab_colour", F.lit("Green"))
    
    # Rename the vendor-specific datetime columns to a unified name
    yellow = yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
                   
    green = green.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                 .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
    
    # Union by name. allowMissingColumns=True tells Spark to automatically fill 
    # Green's missing columns (like Airport_fee) and Yellow's missing columns 
    combined_df = yellow.unionByName(green, allowMissingColumns=True)
    
    return combined_df

def add_zone_names(trips_df, zones_df):
    # Prep the pickup zone lookup
    pu_zones = zones_df.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone")
    )
    
    # Prep the dropoff zone lookup
    do_zones = zones_df.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone")
    )
    
    # Broadcast join because taxi_zone_lookup.csv is only ~265 rows.
    enriched_df = trips_df.join(F.broadcast(pu_zones), on="PULocationID", how="left") \
                          .join(F.broadcast(do_zones), on="DOLocationID", how="left")
                          
    return enriched_df