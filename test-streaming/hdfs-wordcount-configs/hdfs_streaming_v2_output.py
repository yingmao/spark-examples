#!/usr/bin/env python3
"""
VARIATION 2: Output Mode Comparison (FIXED)
====================================
KEY LEARNING: Spark Streaming output mode constraints
FIXED ISSUE: Conditional sorting based on output mode

OUTPUT MODES EXPLAINED:
- complete: Shows ALL accumulated results (sorting allowed)
- update:   Shows ONLY changed results (sorting NOT allowed)

REAL-WORLD ANALOGY:
- complete: "Show me the top 10 trending topics of all time"
- update:   "Alert me when any topic's popularity changes"
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
    print(f"📤 Output mode: {OUTPUT_MODE.upper()}")
    
    if OUTPUT_MODE == "complete":
        print("📊 Shows: ALL words with their cumulative counts")
        print("🔍 Effect: Same words appear every batch with updated totals")
        print("✅ Sorting: ALLOWED (full result set available)")
        print("💡 Use case: Real-time dashboard showing running totals")
    else:
        print("📊 Shows: ONLY words that appeared in current batch")
        print("🔍 Effect: Words disappear when not in current file")
        print("❌ Sorting: NOT ALLOWED (partial results only)")
        print("💡 Use case: Change detection and alerting systems")
    
    print("="*70 + "\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    words = lines.select(
        explode(split(col("value"), r"\s+")).alias("word")
    ).filter(col("word") != "")

    # Base aggregation
    word_counts = words.groupBy("word").count()

    # CRITICAL FIX: Conditional sorting based on output mode
    # This is a fundamental Spark Streaming constraint
    if OUTPUT_MODE == "complete":
        # Safe: Complete mode emits full result set every batch
        final_df = word_counts.orderBy(col("count").desc())
        print("🔧 Applied sorting: Results ordered by count (descending)")
    else:
        # Required: Update mode only emits changed rows - cannot sort
        final_df = word_counts
        print("🔧 No sorting applied: Update mode shows changes only")

    query = final_df.writeStream \
        .outputMode(OUTPUT_MODE) \
        .format("console") \
        .option("numRows", 15) \
        .option("truncate", False) \
        .trigger(processingTime="8 seconds") \
        .start()

    print(f"\n🚀 Streaming with '{OUTPUT_MODE}' output mode...")
    if OUTPUT_MODE == "update":
        print("   Watch: Words appear/disappear based on current file content!")
    else:
        print("   Watch: All words always visible, sorted by frequency!")
    print()

    try:
        query.awaitTermination(120)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • '{OUTPUT_MODE}' mode: {['Partial results only', 'Complete results'][OUTPUT_MODE=='complete']}")
    print(f"   • Sorting allowed: {['No', 'Yes'][OUTPUT_MODE=='complete']}")
    print(f"   • Internal state: Always maintained regardless of output mode")
    print(f"\n🔬 Experiment: Change OUTPUT_MODE to '{['complete', 'update'][OUTPUT_MODE=='complete']}' and compare!")

if __name__ == "__main__":
    main()
