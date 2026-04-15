# example4_kmeans_pipeline.py
"""
Complete MLlib K-Means Pipeline using Spark DataFrame API.
Replaces: Iterative MapReduce K-Means with repeated HDFS reads per iteration
With:     MLlib K-Means with single HDFS read and in-memory iterations
"""

from __future__ import print_function
import sys
import time

# SparkSession and DataFrame imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum

# MLlib imports
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: example4_kmeans_pipeline.py <customer_data> <output_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("Example4_KMeansPipeline") \
        .getOrCreate()

    print("=== Example 4: Complete MLlib K-Means Pipeline ===\n")

    # 1. Read customer behavior data
    raw_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

    print("1. Raw customer data:")
    raw_df.show(10)
    print(f"   Total records: {raw_df.count()}")

    # 2. RFM Feature Engineering (Lab 3 normalization formulas)
    rfm_df = raw_df \
        .withColumn("recency_norm",
            when(col("recency") / 90.0 > 1.0, 1.0).otherwise(col("recency") / 90.0)) \
        .withColumn("frequency_norm",
            when(col("frequency") / 50.0 > 1.0, 1.0).otherwise(col("frequency") / 50.0)) \
        .withColumn("monetary_norm",
            when(col("monetary") / 5000.0 > 1.0, 1.0).otherwise(col("monetary") / 5000.0)) \
        .filter(col("recency").isNotNull() &
                col("frequency").isNotNull() &
                col("monetary").isNotNull())

    print("\n2. Normalized RFM features:")
    rfm_df.select("customer_id", "recency_norm", "frequency_norm", "monetary_norm").show(10)

    # 3. Feature vector assembly (required for MLlib)
    assembler = VectorAssembler(
        inputCols=["recency_norm", "frequency_norm", "monetary_norm"],
        outputCol="features"
    )

    # Cache the dataset since K-Means is iterative
    dataset = assembler.transform(rfm_df).cache()

    print("\n3. Training K-Means model...")
    start_time = time.time()

    # Configure K-Means (same parameters as Lab 2 for comparison)
    kmeans = KMeans(k=4, seed=42, maxIter=15, tol=0.001)
    model = kmeans.fit(dataset)

    training_time = time.time() - start_time

    # Make predictions
    predictions = model.transform(dataset)

    print(f"Training completed in {training_time:.2f} seconds")
    print(f"Converged after {model.summary.numIter} iterations")

    # 4. Display cluster centers
    print("\n4. Cluster Centers:")
    centers = model.clusterCenters()
    for i, center in enumerate(centers):
        print(f"Cluster {i}: R={center[0]:.3f}, F={center[1]:.3f}, M={center[2]:.3f}")

    # 5. Analyze cluster characteristics
    print("\n5. Cluster Analysis:")
    cluster_analysis = predictions.groupBy("prediction") \
        .agg(
            count("*").alias("num_customers"),
            avg("recency_norm").alias("avg_recency"),
            avg("frequency_norm").alias("avg_frequency"),
            avg("monetary_norm").alias("avg_monetary")
        ) \
        .orderBy("prediction")

    cluster_analysis.show()

    # 6. Business interpretation
    print("\n6. Business Segments:")
    segment_names = ["Champions", "Potential Loyalists", "At Risk", "New/Casual"]
    for i, name in enumerate(segment_names):
        print(f"Cluster {i}: {name}")

    # 7. Save results
    final_output = predictions.select("customer_id", "recency", "frequency", "monetary", "prediction")
    final_output.write.mode("overwrite").csv(sys.argv[2], header=True)

    # 8. Performance summary
    print(f"\n=== Performance Summary ===")
    print(f"Total customers processed: {dataset.count()}")
    print(f"Training time: {training_time:.2f} seconds")
    print(f"Iterations to convergence: {model.summary.numIter}")
    print("Compare this to your Lab 2 MapReduce implementation!")

    dataset.unpersist()
    spark.stop()
