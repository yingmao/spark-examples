# example2_broadcast_join.py
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, sum as spark_sum, count, desc
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: example2_broadcast_join.py <violations_path> <output_path>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Example2_BroadcastJoin")\
        .getOrCreate()

    print("=== Example 2: Broadcast Join Revenue Analysis ===\n")

    # Read violations data
    violations_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

    # Create small lookup table (like Lab 3 fine amounts)
    fines_data = [
        ("21", 45),   # Street Cleaning
        ("14", 115),  # No Standing
        ("20", 65),   # No Parking
        ("38", 35)    # Meter Expired
    ]
    fines_df = spark.createDataFrame(fines_data, ["violation_code", "fine_amount"])

    print("1. Fine lookup table:")
    fines_df.show()

    # Broadcast join - small table distributed to all nodes
    revenue_df = violations_df.join(
        broadcast(fines_df),
        "violation_code",
        "left"
    ).fillna(50, subset=["fine_amount"])  # Default fine for unknown codes

    print("\n2. Sample joined data:")
    revenue_df.show(10)

    # Revenue analysis by location (Lab 3 hotspot pattern)
    hotspots = revenue_df.groupBy("street_code") \
        .agg(
            count("*").alias("total_tickets"),
            spark_sum("fine_amount").alias("total_revenue"),
            (count("*") / 365.0).alias("tickets_per_day")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(10)

    print("\n3. Top 10 revenue locations:")
    hotspots.show()

    # Save results
    hotspots.write.mode("overwrite").csv(sys.argv[2], header=True)

    spark.stop()
