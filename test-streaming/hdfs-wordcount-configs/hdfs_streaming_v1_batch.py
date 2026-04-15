#!/usr/bin/env python3
"""
VARIATION 1: Batch Granularity Control
=====================================
KEY PARAMETER: maxFilesPerTrigger
LEARNING GOAL: Understanding batch sizing trade-offs

EXPERIMENT SETTINGS:
- maxFilesPerTrigger=1 → 8 small batches (fine granularity)
- maxFilesPerTrigger=2 → 4 medium batches (balanced)  
- maxFilesPerTrigger=4 → 2 large batches (coarse granularity)

REAL-WORLD ANALOGY:
Like adjusting assembly line batch sizes:
- Small batches: More frequent updates, higher overhead
- Large batches: Less frequent updates, better throughput
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # STUDENT EXPERIMENT: Try changing this value!
    FILES_PER_BATCH = 2  # Change to 1, 2, 4, or 8
    
    spark = SparkSession.builder \
        .appName(f"Variation1-BatchSize-{FILES_PER_BATCH}files") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 1: Batch Granularity Experiment")
    print("="*70)
    print(f"📁 Files per batch: {FILES_PER_BATCH}")
    print(f"📊 Expected batches: {8 // FILES_PER_BATCH} + {8 % FILES_PER_BATCH if 8 % FILES_PER_BATCH else 0}")
    print("🔍 Watch how word counts grow in different-sized jumps!")
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", FILES_PER_BATCH) \
        .load("/streaming-test/input")

    words = lines.select(
        explode(split(col("value"), r"\s+")).alias("word")
    ).filter(col("word") != "")

    word_counts = words \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())

    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 12) \
        .option("truncate", False) \
        .trigger(processingTime="8 seconds") \
        .start()

    print(f"🚀 Processing {FILES_PER_BATCH} files per batch...")
    print("   Notice the different growth patterns!\n")

    try:
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • Processed 8 files in ~{8 // FILES_PER_BATCH + (1 if 8 % FILES_PER_BATCH else 0)} batches")
    print(f"   • Larger batches = fewer updates but bigger jumps")
    print(f"   • Smaller batches = more frequent updates, finer granularity")

if __name__ == "__main__":
    main()
