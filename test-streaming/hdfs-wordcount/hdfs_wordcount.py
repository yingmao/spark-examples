#!/usr/bin/env python3
"""
Simple HDFS Spark Streaming Demo for Students

Key Concept: Even though we uploaded 5 files before starting Spark,
maxFilesPerTrigger=1 makes Spark process them one at a time,
creating 5 distinct "streaming batches" that students can observe.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Student-HDFS-Streaming-Demo") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    # Reduce log noise so students see clean output
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n" + "="*60)
    print("    SPARK STREAMING DEMO - HDFS Word Count")
    print("="*60)
    print("📁 Reading from: /streaming-test/input")
    print("⚡ Processing: 1 file per batch (maxFilesPerTrigger=1)")
    print("📊 Output: Console (watch word counts grow!)")
    print("="*60 + "\n")
    
    # Read streaming data from HDFS
    # KEY: maxFilesPerTrigger=1 processes files one by one
    lines = spark.readStream \
        .format("text") \
        .option("maxFilesPerTrigger", 1) \
        .load("/streaming-test/input")
    
    # Split lines into words
    words = lines.select(
        explode(split(col("value"), r"\s+")).alias("word")
    ).filter(col("word") != "")
    
    # Count words (complete mode shows running totals)
    word_counts = words \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())
    
    # Output to console
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 20) \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print("🚀 Streaming started! Watch for 5 batches...")
    print("   (First batch takes ~60-90 seconds to initialize)")
    print("   (Remaining batches will be much faster)\n")
    
    try:
        # Run for 2 minutes - plenty of time for 5 batches
        query.awaitTermination(240)
    except KeyboardInterrupt:
        print("\n⏹️  Stopped by user")
    finally:
        query.stop()
        spark.stop()
        print("\n✅ Streaming demo completed!")

if __name__ == "__main__":
    main()
