#!/usr/bin/env python3
"""
VARIATION 3: Tumbling Window Aggregation (FIXED)
========================================
KEY LEARNING: Deterministic operations in streaming
FIXED ISSUE: Replaced banned monotonically_increasing_id() with input_file_name()

STREAMING CONSTRAINT LESSON:
Spark bans non-deterministic functions in streaming to guarantee
that crashed jobs can replay data and get identical results.
Solution: Extract metadata deterministically from the data itself!

TUMBLING WINDOWS CONCEPT:
|-- Window 1 --|-- Window 2 --|-- Window 3 --|-- Window 4 --|
0-45s          45-90s         90-135s        135-180s

Each window is independent - word counts reset per window.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, window,
    unix_timestamp, from_unixtime, lit, 
    input_file_name, regexp_extract
)

def main():
    spark = SparkSession.builder \
        .appName("Variation3-TumblingWindows-Deterministic") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 3: Tumbling Window Experiment")
    print("="*70)
    print("⏰ Window size: 45 seconds (tumbling, no overlap)")
    print("📊 File timing: 50 seconds apart (spans multiple windows)")
    print("")
    print("🎓 Streaming Concept Highlight:")
    print("   ❌ monotonically_increasing_id() → Non-deterministic (BANNED)")
    print("   ✅ input_file_name() + regex → Deterministic (SAFE)")
    print("   💡 Fault tolerance requires repeatable results!")
    print("")
    print("Expected Window Distribution:")
    print("  Window 1 [10:00:45-10:01:30]: batch_01.txt (Spark content)")
    print("  Window 2 [10:01:30-10:02:15]: batch_02.txt (Hadoop content)")
    print("  Window 3 [10:02:15-10:03:00]: batch_03.txt (YARN content)")
    print("  Window 4 [10:03:00-10:03:45]: batch_04.txt (Python content)")
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    # CRITICAL FIX: Extract deterministic ID from filename
    # input_file_name() returns: "hdfs://.../streaming-test/input/batch_01.txt"
    # regexp_extract grabs the "01" and casts to integer
    lines_with_id = lines.withColumn(
        "file_id", 
        regexp_extract(input_file_name(), r'batch_(\d+)\.txt', 1).cast("int")
    )

    # Create mock timestamps based on the extracted file_id
    # File 1 → 10:00:50, File 2 → 10:01:40, File 3 → 10:02:30, etc.
    # Each file falls into a different 45-second window
    timestamped_lines = lines_with_id.withColumn(
        "mock_timestamp",
        from_unixtime(
            unix_timestamp(lit("2024-01-01 10:00:00")) + (col("file_id") * 50)
        ).cast("timestamp")
    )

    # Extract words with their event times
    words = timestamped_lines.select(
        explode(split(col("value"), r"\s+")).alias("word"),
        col("mock_timestamp").alias("event_time"),
        col("file_id")  # Keep for debugging
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
    print("   Each window shows independent word counts (not cumulative).\n")

    try:
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • Streaming requires deterministic operations for fault tolerance")
    print(f"   • input_file_name() provides safe, repeatable metadata extraction")
    print(f"   • Each 45-second window is completely independent")
    print(f"   • Windowing enables 'last X seconds' analysis patterns")

if __name__ == "__main__":
    main()
