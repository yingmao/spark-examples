# bisecting_kmeans_df.py
"""
Bisecting K-Means using Spark ML Pipeline with DataFrames.
Replaces: pyspark.mllib.clustering.BisectingKMeans (RDD-based)
With: pyspark.ml.clustering.BisectingKMeans (DataFrame-based)
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bisecting_kmeans_df.py <input_libsvm_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("BisectingKMeansDataFrame") \
        .getOrCreate()

    print("=== Bisecting K-Means with Spark ML DataFrame API ===\n")

    # 1. Load data in LIBSVM format (same as your existing examples)
    dataset = spark.read.format("libsvm").load(sys.argv[1])
    
    print("1. Input data:")
    dataset.show(5, truncate=False)
    print(f"   Total records: {dataset.count()}")

    # 2. Configure Bisecting K-Means (replaces BisectingKMeans.train())
    bkm = BisectingKMeans(
        featuresCol="features",
        predictionCol="prediction", 
        k=2,
        maxIter=5,
        seed=1
    )

    # 3. Fit model (replaces direct .train() call)
    print("\n2. Training Bisecting K-Means model...")
    model = bkm.fit(dataset)

    # 4. Make predictions
    predictions = model.transform(dataset)
    print("\n3. Sample predictions:")
    predictions.select("features", "prediction").show(10, truncate=False)

    # 5. Evaluate clustering (replaces model.computeCost())
    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="prediction"
    )
    silhouette = evaluator.evaluate(predictions)
    print(f"\n4. Silhouette score = {silhouette:.6f}")

    # 6. Display cluster centers (same as original)
    centers = model.clusterCenters()
    print("\n5. Cluster Centers:")
    for i, center in enumerate(centers):
        print(f"   Cluster {i}: {center}")

    # 7. Cluster distribution
    print("\n6. Cluster size distribution:")
    predictions.groupBy("prediction").count().orderBy("prediction").show()

    spark.stop()
