# example1_basic_dataframe.py
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: example1_basic_dataframe.py <input_path> <output_path>")
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Example1_BasicDataFrame")\
        .getOrCreate()

    print("=== Example 1: Basic DataFrame Operations ===\n")

    # Define explicit schema (Lab 3 best practice)
    schema = StructType([
        StructField("plate_id", StringType(), True),
        StructField("vehicle_color", StringType(), True),
        StructField("state", StringType(), True),
        StructField("violation_code", StringType(), True)
    ])

    # Read with explicit schema
    df = spark.read.csv(sys.argv[1], header=True, schema=schema)

    print("1. Original data:")
    df.show(10)
    print(f"Total records: {df.count()}")

    # Data cleaning pipeline (similar to Lab 3 Task 1.1)
    cleaned_df = df \
        .withColumn("vehicle_color",
            when(upper(col("vehicle_color")).isin("BLK", "BLACK"), "BLACK")
            .when(upper(col("vehicle_color")).isin("WH", "WHITE", "WHT"), "WHITE")
            .otherwise(upper(col("vehicle_color")))) \
        .withColumn("state",
            when(upper(col("state")).isin("NY", "NEW YORK"), "NY")
            .otherwise(upper(col("state")))) \
        .filter(col("plate_id").isNotNull() & col("violation_code").isNotNull()) \
        .cache()  # Cache for reuse

    print("\n2. After cleaning:")
    cleaned_df.show(10)

    # Basic aggregation
    color_stats = cleaned_df.groupBy("vehicle_color") \
        .agg(count("*").alias("count")) \
        .orderBy(col("count").desc())

    print("\n3. Color distribution:")
    color_stats.show()

    # Save results
    color_stats.write.mode("overwrite").csv(sys.argv[2], header=True)

    cleaned_df.unpersist()
    spark.stop()
