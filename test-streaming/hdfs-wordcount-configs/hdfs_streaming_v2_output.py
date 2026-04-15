#!/usr/bin/env python3
"""
VARIATION 2: Output Mode Comparison
==================================
KEY PARAMETER: outputMode
LEARNING GOAL: Understanding result emission strategies

OUTPUT MODES EXPLAINED:
- complete: Shows ALL accumulated results (current behavior)
- update:   Shows ONLY changed results since last batch
- append:   Shows ONLY new results (not applicable for aggregations)

BUSINESS USE CASE:
- complete: Dashboard showing "Top 10 words of all time"
- update:   Network alert showing "Words with changed counts"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # STUDENT EXPERIMENT: Try changing this!
    OUTPUT_MODE = "update"  # Change to "complete" or "update"
    
    spark = SparkSession.builder \
        .appName(f"Variation2-OutputMode-{OUTPUT_MODE}") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 2: Output Mode Experiment")
    print("="*70)
    print(f"📤 Output mode: {OUTPUT_MODE}")
    if OUTPUT_MODE == "complete":
        print("📊 Shows: ALL words with their total counts")
        print("🔍 Effect: Same word appears in every batch (if count > 0)")
    else:
        print("📊 Shows: ONLY words whose counts CHANGED this batch")
        print("🔍 Effect: Words disappear from output when not in current files")
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    words = lines.select(
        explode(split(col("value"), r"\s+")).alias("word")
    ).filter(col("word") != "")

    word_counts = words \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())

    query = word_counts.writeStream \
        .outputMode(OUTPUT_MODE) \
        .format("console") \
        .option("numRows", 15) \
        .option("truncate", False) \
        .trigger(processingTime="8 seconds") \
        .start()

    print(f"🚀 Streaming with '{OUTPUT_MODE}' output mode...")
    if OUTPUT_MODE == "update":
        print("   Notice: Words appear/disappear based on current batch content!\n")
    else:
        print("   Notice: All words always visible (if count > 0)\n")

    try:
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • '{OUTPUT_MODE}' mode changes what data is emitted")
    print(f"   • Internal state is always maintained regardless of output mode")
    print(f"   • Choose output mode based on downstream system requirements")

if __name__ == "__main__":
    main()
