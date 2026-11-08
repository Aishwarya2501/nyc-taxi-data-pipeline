import pyspark.sql.functions as F


def run_quality_checks(df, expected_start, expected_end):
# DQC checks which checks for violations and validations needed for the queries built from the cleaned data
    tagged = (
        df
        # 1. dropoff must be after pickup
        .withColumn("fail_time",
            F.when(F.col("dropoff_datetime") <= F.col("pickup_datetime"),
                   "dropoff_before_pickup"))
        # 2. a $0 fare is fine, a negative one is not
        .withColumn("fail_fare",
            F.when(F.col("fare_amount") < 0, "negative_fare"))
        # 3. pickup must fall inside the expected window
        .withColumn("fail_date",
            F.when((F.col("pickup_datetime") < F.lit(expected_start)) |
                   (F.col("pickup_datetime") >= F.lit(expected_end)),
                   "outside_expected_date_range"))
        # 4. pickup zone must be filled in
        .withColumn("fail_zone",
            F.when(F.col("PULocationID").isNull(), "missing_pickup_location"))
        # 5. distance must be positive and sane
        .withColumn("fail_dist",
            F.when((F.col("trip_distance") <= 0) | (F.col("trip_distance") > 1000),
                   "invalid_trip_distance"))
        # join all the tags into one string (concat_ws skips the NULLs)
        .withColumn("rejection_reason",
            F.concat_ws(", ", "fail_time", "fail_fare", "fail_date", "fail_zone", "fail_dist"))
        # a clean trip ends up with an empty string here - turn it back into NULL
        .withColumn("rejection_reason",
            F.when(F.col("rejection_reason") != "", F.col("rejection_reason")))
        .drop("fail_time", "fail_fare", "fail_date", "fail_zone", "fail_dist")
    )

    clean_df = tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    quarantine_df = tagged.filter(F.col("rejection_reason").isNotNull())

    return clean_df, quarantine_df


def summarize_quarantine(quarantine_df):
    """Returns row counts grouped by failure reason for pipeline observability."""
    return (
        quarantine_df
        .groupBy("rejection_reason")
        .count()
        .orderBy(F.col("count").desc())
    )