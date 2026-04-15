#!/usr/bin/env python3
"""
VARIATION 3: Tumbling Window Aggregation
========================================
KEY LEARNING: Time-based data grouping in streaming
CONCEPT: Independent time windows vs global accumulation

TUMBLING WINDOWS CONCEPT:
|-- Window 1 --|-- Window 2 --|-- Window 3 --|-- Window 4 --|
0-45s          45-90s         90-135s        135-180s

Each window is completely independent:
- Word counts reset for each new window  
- Same word can appear in multiple windows with different counts
- Perfect for "What happened in the last X seconds?" analysis

COMPARISON WITH V1 (Complete Mode):
V1: spark count grows 2 → 2 → 2 → 4 → 4 (accumulates forever)
V3: spark count is 2 in Window 1, 0 in Window 2, 2 in Window 4 (independent)

STREAMING CONSTRAINT LESSON:
Spark bans non-deterministic functions to guarantee fault tolerance:
  ❌ monotonically_increasing_id() → Non-deterministic (BANNED)
  ✅ input_file_name() + regex    → Deterministic (SAFE)

FILE TO WINDOW MAPPING (50 seconds apart, 45-second windows):
  batch_01.txt → file_id=1 → timestamp=10:00:50 → Window [10:00:45-10:01:30]
  batch_02.txt → file_id=2 → timestamp=10:01:40 → Window [10:01:30-10:02:15]  
  batch_03.txt → file_id=3 → timestamp=10:02:30 → Window [10:02:15-10:03:00]
  batch_04.txt → file_id=4 → timestamp=10:03:20 → Window [10:03:00-10:03:45]
  ... and so on for all 8 files

REAL-WORLD USE CASES:
- "Trending hashtags in the last 5 minutes"
- "Error rate per hour" 
- "Network traffic per 30-second interval"
- "Sales volume per day"
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, col, window,
    unix_timestamp, from_unixtime, lit,
    input_file_name, regexp_extract
)

def main():
    # ================================================================
    # STEP 1: Initialize Spark with cluster-optimized settings
    # ================================================================
    spark = SparkSession.builder \
        .appName("Variation3-TumblingWindows-Educational") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 3: Tumbling Window Experiment")
    print("="*70)
    print("⏰ Window size:  45 seconds (tumbling, no overlap)")
    print("📊 File timing:  50 seconds apart (each file in different window)")
    print("📁 Files:        batch_01.txt to batch_08.txt")
    print("")
    print("🎓 Streaming Constraint Lesson:")
    print("   ❌ monotonically_increasing_id() → Non-deterministic (BANNED)")
    print("   ✅ input_file_name() + regex    → Deterministic (SAFE)")
    print("   💡 Fault tolerance requires repeatable results on replay!")
    print("")
    print("🗓️  Expected Window Distribution:")
    print("  Window 1 [10:00:45-10:01:30] ← batch_01 (Spark content)")
    print("  Window 2 [10:01:30-10:02:15] ← batch_02 (Hadoop content)")  
    print("  Window 3 [10:02:15-10:03:00] ← batch_03 (YARN content)")
    print("  Window 4 [10:03:00-10:03:45] ← batch_04 (Python content)")
    print("  ... continues for all 8 files")
    print("")
    print("🔍 Key Difference from V1:")
    print("   V1: 'spark' count accumulates 2→2→2→4→4 (global state)")
    print("   V3: 'spark' count resets per window (independent buckets)")
    print("="*70 + "\n")

    # ================================================================
    # STEP 2: Read streaming text files from HDFS  
    # ================================================================
    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    # ================================================================
    # STEP 3: Extract deterministic file ID from filename
    # ================================================================
    # CRITICAL: input_file_name() is deterministic and streaming-safe
    # Returns: "hdfs://10.128.0.6:9000/streaming-test/input/batch_01.txt"
    # regexp_extract grabs batch number: "01" → cast to int → 1
    lines_with_id = lines.withColumn(
        "file_id",
        regexp_extract(input_file_name(), r'batch_(\d+)\.txt', 1).cast("int")
    )

    # ================================================================
    # STEP 4: Create mock event-time timestamps
    # ================================================================
    # Each file gets timestamp 50 seconds after previous:
    #   File 1 → base + (1 × 50s) = 10:00:50 → Window [10:00:45-10:01:30]
    #   File 2 → base + (2 × 50s) = 10:01:40 → Window [10:01:30-10:02:15]
    #
    # SPARK 4.1.1 FIX: Cannot add integer directly to timestamp
    # Solution: timestamp → unix seconds → add integer → convert back
    timestamped_lines = lines_with_id.withColumn(
        "event_time",
        from_unixtime(
            unix_timestamp(lit("2024-01-01 10:00:00")) + (col("file_id") * 50)
        ).cast("timestamp")
    )

    # ================================================================
    # STEP 5: Split lines into words with event timestamps
    # ================================================================
    words = timestamped_lines.select(
        explode(split(col("value"), r"\s+")).alias("word"),
        col("event_time")
    ).filter(col("word") != "")

    # ================================================================
    # STEP 6: Apply tumbling window aggregation
    # ================================================================
    # window("45 seconds") creates non-overlapping time buckets
    # Each word is counted independently within each bucket
    windowed_counts = words \
        .groupBy(
            window(col("event_time"), "45 seconds"),
            col("word")
        ) \
        .count() \
        .orderBy(col("window"), col("count").desc())

    # ================================================================
    # STEP 7: Start streaming query with console output
    # ================================================================
    query = windowed_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 25) \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()

    print("🚀 Streaming started! Watch for 8 batches across 8 windows...")
    print("   First batch takes ~20-30 seconds (cold start)")
    print("   Remaining batches will be much faster")
    print("   Notice: Each window shows INDEPENDENT word counts!\n")

    # ================================================================
    # STEP 8: Run for sufficient time to process all files
    # ================================================================
    try:
        # 3 minutes allows cold start + processing all 8 files
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        # ================================================================
        # STEP 9: GRACEFUL SHUTDOWN (Eliminates network errors)
        # ================================================================
        print("\n" + "="*50)
        print("Initiating graceful shutdown...")
        
        # Stop streaming query first (prevents new data processing)
        if query and query.isActive:
            try:
                query.stop()
                print("✓ Streaming query stopped cleanly")
            except Exception:
                pass
                
        # Critical: Allow pending network operations to complete
        # This prevents "StacklessClosedChannelException" errors
        time.sleep(2)
        
        # Stop Spark context after network cleanup
        try:
            spark.stop()
            print("✓ Spark session closed cleanly")
        except Exception:
            pass
            
        print("="*50)

    # ================================================================
    # EDUCATIONAL SUMMARY
    # ================================================================
    print(f"\n🎓 Learning Summary:")
    print(f"   • Streaming requires DETERMINISTIC operations for fault tolerance")
    print(f"   • input_file_name() provides safe, repeatable metadata extraction")
    print(f"   • Each 45-second window maintains INDEPENDENT word counts")
    print(f"   • Same word can appear in multiple windows with different counts")
    print(f"   • Windowing enables 'What happened in the last X seconds?' analysis")
    print(f"")
    print(f"🔬 Student Experiment Ideas:")
    print(f"   • Change window size: window('30 seconds') or window('90 seconds')")
    print(f"   • Change file spacing: col('file_id') * 30 or col('file_id') * 100")
    print(f"   • Try sliding windows: window('60 seconds', '30 seconds')")

if __name__ == "__main__":
    main()
