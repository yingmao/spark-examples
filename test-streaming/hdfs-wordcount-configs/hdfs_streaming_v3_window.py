#!/usr/bin/env python3
"""
VARIATION 3: Tumbling Window Aggregation (FIXED)
========================================
KEY LEARNING: Time-based data grouping in streaming
FIXED ISSUE: Proper timestamp arithmetic for Spark 4.1.1

TUMBLING WINDOWS CONCEPT:
|-- Window 1 --|-- Window 2 --|-- Window 3 --|-- Window 4 --|
0-45s          45-90s         90-135s        135-180s

Each window is independent - word counts reset per window
Files spread across windows to demonstrate time-based grouping

REAL-WORLD USE CASES:
- "Trending hashtags in the last 5 minutes"
- "Error rate per hour"
- "Sales volume per day"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, window,
    unix_timestamp, from_unixtime, lit, 
    monotonically_increasing_id
)

def main():
    spark = SparkSession.builder \
        .appName("Variation3-TumblingWindows") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 3: Tumbling Window Experiment")
    print("="*70)
    print("⏰ Window size: 45 seconds (tumbling, no overlap)")
    print("📊 File timing: 50 seconds apart (spans multiple windows)")
    print("")
    print("Expected Window Distribution:")
    print("  Window 1 [10:00:00-10:00:45]: Files 1")
    print("  Window 2 [10:00:45-10:01:30]: Files 2") 
    print("  Window 3 [10:01:30-10:02:15]: Files 3")
    print("  Window 4 [10:02:15-10:03:00]: Files 4")
    print("  ... and so on")
    print("")
    print("🔍 Key Difference from V1 (complete mode):")
    print("   V1: 'spark' count grows 2→2→2→4→4 (accumulates forever)")
    print("   V3: 'spark' count resets per window (2 in window 1, 2 in window 4)")
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    # CRITICAL FIX: Safe timestamp arithmetic for Spark 4.1.1
    # Cannot add integers directly to timestamps - must convert to/from unix time
    timestamped_lines = lines \
        .withColumn("file_id", monotonically_increasing_id()) \
        .withColumn(
            "mock_timestamp",
            # Step 1: Convert base timestamp to unix seconds (integer)
            # Step 2: Add file_id * 50 seconds to spread files across time
            # Step 3: Convert back to timestamp format
            from_unixtime(
                unix_timestamp(lit("2024-01-01 10:00:00")) + (col("file_id") * 50)
            ).cast("timestamp")
        )

    # Extract words with their event times
    words = timestamped_lines.select(
        explode(split(col("value"), r"\s+")).alias("word"),
        col("mock_timestamp").alias("event_time")
    ).filter(col("word") != "")

    # Apply tumbling window: 45-second windows, no overlap
    windowed_counts = words \
        .groupBy(
            window(col("event_time"), "45 seconds"),
            col("word")
        ) \
        .count() \
        .orderBy(col("window"), col("count").desc())

    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 25) \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("🚀 Processing with 45-second tumbling windows...")
    print("   Watch how files fall into different time windows!")
    print("   Each window shows independent word counts.\n")

    try:
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • Each 45-second window is completely independent")
    print(f"   • Same word can appear in multiple windows with different counts")
    print(f"   • Useful for 'What happened in the last X seconds?' analysis")
    print(f"   • Spark 4.1.1: Use unix_timestamp() for safe date arithmetic")

if __name__ == "__main__":
    main()
