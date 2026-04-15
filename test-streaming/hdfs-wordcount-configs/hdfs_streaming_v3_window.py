#!/usr/bin/env python3
"""
VARIATION 3: Tumbling Window Aggregation  
========================================
KEY PARAMETER: window() function
LEARNING GOAL: Understanding time-based data grouping

TUMBLING WINDOWS CONCEPT:
|-- Window 1 --|-- Window 2 --|-- Window 3 --|
0s            30s            60s            90s

Each window is independent - counts reset for each window
Different from complete mode which accumulates forever

REAL-WORLD USE CASES:
- "Trending hashtags in the last 5 minutes"
- "Error rate per hour" 
- "Sales volume per day"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, window, 
    unix_timestamp, to_timestamp, lit
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
    print("⏰ Window size: 45 seconds (tumbling)")
    print("📊 Effect: Counts reset every 45 seconds")
    print("🔍 Compare: Complete mode accumulates forever")
    print("           Windowed mode resets each window")
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    # Add mock timestamps that spread across different windows
    # This simulates realistic event-time data
    from pyspark.sql.functions import monotonically_increasing_id, when

    timestamped_lines = lines \
        .withColumn("file_id", monotonically_increasing_id()) \
        .withColumn("mock_timestamp", 
            # Spread files across different 45-second windows
            to_timestamp(lit("2024-01-01 10:00:00")) + 
            (col("file_id") * 50).cast("long")  # 50 seconds apart
        )

    words = timestamped_lines.select(
        explode(split(col("value"), r"\s+")).alias("word"),
        col("mock_timestamp").alias("event_time")
    ).filter(col("word") != "")

    # Apply tumbling window: 45-second windows, no overlap
    windowed_counts = words \
        .groupBy(
            window(col("event_time"), "45 seconds"),  # Window size only = tumbling
            col("word")
        ) \
        .count() \
        .orderBy(col("window"), col("count").desc())

    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 20) \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("🚀 Processing with 45-second tumbling windows...")
    print("   Notice: Each window shows independent counts!\n")

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
    print(f"   • Useful for 'What happened in the last X seconds?' questions")

if __name__ == "__main__":
    main()
