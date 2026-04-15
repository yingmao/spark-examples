#!/usr/bin/env python3
"""
HDFS-based Spark Structured Streaming example.
Monitors HDFS directory for new text files and performs real-time word count.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, current_timestamp

# Configuration
HDFS_INPUT_DIR = "hdfs:///streaming-test/input"
HDFS_OUTPUT_DIR = "hdfs:///streaming-test/output"
CHECKPOINT_DIR = "hdfs:///spark-checkpoints/hdfs-streaming"

# Default run duration (seconds) - 0 means run until manually stopped
DEFAULT_DURATION = 60


def main() -> None:
    # Get duration from command line argument
    duration = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DURATION
    
    spark = SparkSession.builder \
        .appName("HDFS-Structured-Streaming-WordCount") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("=== HDFS Structured Streaming Started ===")
    print(f"Monitoring: {HDFS_INPUT_DIR}")
    print(f"Output:     {HDFS_OUTPUT_DIR}")
    print(f"Duration:   {duration} seconds")
    print("=" * 50)

    try:
        # Read streaming text files from HDFS directory
        lines = spark.readStream \
            .format("text") \
            .option("maxFilesPerTrigger", 1) \
            .load(HDFS_INPUT_DIR)

        # Transform: Split lines into words and filter out empty strings
        words = lines.select(
            explode(split(col("value"), r"\s+")).alias("word")
        ).filter(col("word") != "")

        # Aggregate: Count words across all processed files
        word_counts = words.groupBy("word") \
            .count() \
            .orderBy(col("count").desc())

        # Output 1: Console for real-time monitoring
        console_query = word_counts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 15) \
            .trigger(processingTime="8 seconds") \
            .start()

        # Output 2: HDFS for persistence (optional)
        hdfs_query = word_counts.writeStream \
            .outputMode("complete") \
            .format("csv") \
            .option("path", HDFS_OUTPUT_DIR) \
            .option("header", "true") \
            .option("checkpointLocation", CHECKPOINT_DIR) \
            .trigger(processingTime="8 seconds") \
            .start()

        print("Streaming queries started. Processing incoming files...")
        print("Watch for new batches appearing in console output.")

        # Run for specified duration
        if duration > 0:
            console_query.awaitTermination(duration)
        else:
            console_query.awaitTermination()

    except KeyboardInterrupt:
        print("\n[INFO] Streaming interrupted by user.")
    except Exception as e:
        print(f"[ERROR] Streaming error: {e}")
    finally:
        # Graceful shutdown
        print("Stopping streaming queries...")
        try:
            console_query.stop()
            hdfs_query.stop()
        except:
            pass
        spark.stop()
        print("=== HDFS Structured Streaming Completed ===")


if __name__ == "__main__":
    main()
