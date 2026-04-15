# example3_window_functions.py
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lag, when, sum as spark_sum,
    min as spark_min, max as spark_max, count
)
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: example3_window_functions.py <events_path> <output_path>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Example3_WindowFunctions")\
        .getOrCreate()

    print("=== Example 3: Session Reconstruction with Window Functions ===\n")

    # Read clickstream events
    events_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

    print("1. Raw events:")
    events_df.show(10)

    # Window specification: partition by user, order by timestamp
    # This replaces MapReduce Secondary Sort completely
    window_spec = Window.partitionBy("user_id").orderBy("timestamp")

    # Calculate session boundaries using 30-minute timeout
    sessions_df = events_df \
        .withColumn(
            "prev_timestamp",
            lag("timestamp").over(window_spec)
        ) \
        .withColumn(
            "time_gap_minutes",
            (col("timestamp").cast("long") - col("prev_timestamp").cast("long")) / 60.0
        ) \
        .withColumn(
            "new_session_flag",
            when(col("prev_timestamp").isNull() | (col("time_gap_minutes") > 30), 1)
            .otherwise(0)
        ) \
        .withColumn(
            "session_id",
            spark_sum("new_session_flag").over(
                window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )
        )

    print("\n2. Events with session IDs:")
    sessions_df.select("user_id", "timestamp", "event_type", "session_id", "time_gap_minutes").show(15)

    # Aggregate session-level metrics
    session_metrics = sessions_df \
        .groupBy("user_id", "session_id") \
        .agg(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time"),
            count("*").alias("event_count"),
            spark_sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
        ) \
        .withColumn("duration_minutes",
            (col("end_time").cast("long") - col("start_time").cast("long")) / 60.0) \
        .withColumn("converted", col("total_revenue") > 0)

    print("\n3. Session metrics:")
    session_metrics.show(10)

    # Calculate global metrics
    total_sessions = session_metrics.count()
    conversion_rate = session_metrics.filter(col("converted")).count() * 100.0 / total_sessions

    print(f"\nTotal sessions: {total_sessions}")
    print(f"Conversion rate: {conversion_rate:.2f}%")

    # Save results
    session_metrics.write.mode("overwrite").csv(sys.argv[2], header=True)

    spark.stop()