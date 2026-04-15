#!/usr/bin/env python3
"""
VARIATION 4: Threshold-Based Filtering
====================================
KEY PARAMETER: filter() after aggregation
LEARNING GOAL: Real-time significance detection

THRESHOLD FILTERING CONCEPT:
Only show words that appear THRESHOLD or more times
Early batches may show empty results until threshold is reached

REAL-WORLD USE CASES:
- Twitter: "Only show hashtags mentioned 100+ times"
- Fraud Detection: "Alert if transaction count > 10 per minute"  
- Network Monitoring: "Flag if error rate > 5%"
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # STUDENT EXPERIMENT: Try different thresholds!
    SIGNIFICANCE_THRESHOLD = 3  # Change to 1, 2, 3, 4, or 5
    
    spark = SparkSession.builder \
        .appName(f"Variation4-Threshold-{SIGNIFICANCE_THRESHOLD}") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("    VARIATION 4: Significance Threshold Experiment")
    print("="*70)
    print(f"🎯 Threshold: Only show words appearing {SIGNIFICANCE_THRESHOLD}+ times")
    print("📊 Effect: Noise reduction, focus on frequent terms")
    print("⚠️  Early batches may be empty (threshold not yet reached)")
    print("="*70)
    print(f"\n🔍 Experiment with different thresholds:")
    print(f"   THRESHOLD = 1  → All words (no filtering)")
    print(f"   THRESHOLD = 2  → Common words only")  
    print(f"   THRESHOLD = 3  → Frequent words only (current: {SIGNIFICANCE_THRESHOLD})")
    print(f"   THRESHOLD = 5  → Very frequent words only\n")

    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")

    words = lines.select(
        explode(split(col("value"), r"\s+")).alias("word")
    ).filter(col("word") != "")

    # First aggregate all words
    all_word_counts = words \
        .groupBy("word") \
        .count()

    # Then filter for significant words only
    significant_words = all_word_counts \
        .filter(col("count") >= SIGNIFICANCE_THRESHOLD) \
        .orderBy(col("count").desc())

    query = significant_words.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 10) \
        .option("truncate", False) \
        .trigger(processingTime="8 seconds") \
        .start()

    print(f"🚀 Filtering for words with {SIGNIFICANCE_THRESHOLD}+ occurrences...")
    print("   Watch vocabulary grow as more words reach the threshold!\n")

    try:
        query.awaitTermination(180)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        
    print(f"\n🎓 Learning Summary:")
    print(f"   • Threshold filtering reduces noise in streaming analytics")
    print(f"   • Higher thresholds = fewer but more significant results")
    print(f"   • Essential for real-time alerting and trend detection")

if __name__ == "__main__":
    main()
