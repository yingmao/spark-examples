# gbt_classification_df.py
"""
Gradient Boosted Tree Classification using Spark ML Pipeline.
Original was already using pyspark.ml - enhanced with parameterization and evaluation.
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: gbt_classification_df.py <input_libsvm_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("GBTClassificationDF") \
        .getOrCreate()

    print("=== Gradient Boosted Trees with ML Pipeline ===\n")

    # 1. Load data
    data = spark.read.format("libsvm").load(sys.argv[1])
    print("1. Input data:")
    data.show(10, truncate=False)
    print(f"   Total records: {data.count()}")

    print("\n2. Label distribution:")
    data.groupBy("label").count().orderBy("label").show()

    # 2. Build Pipeline
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    featureIndexer = VectorIndexer(
        inputCol="features", 
        outputCol="indexedFeatures", 
        maxCategories=4
    ).fit(data)

    (trainingData, testData) = data.randomSplit([0.7, 0.3], seed=42)
    print(f"\n3. Data split - Training: {trainingData.count()}, Test: {testData.count()}")

    gbt = GBTClassifier(
        labelCol="indexedLabel", 
        featuresCol="indexedFeatures", 
        maxIter=10,
        seed=42
    )

    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])

    # 3. Train and predict
    print("\n4. Training GBT Pipeline...")
    model = pipeline.fit(trainingData)
    predictions = model.transform(testData)

    print("5. Sample predictions:")
    predictions.select("prediction", "indexedLabel", "features").show(5, truncate=False)

    # 4. Comprehensive evaluation
    print("6. Model evaluation:")
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    accuracy = accuracy_evaluator.evaluate(predictions)
    print(f"   Accuracy: {accuracy:.4f}")
    print(f"   Test Error: {(1.0 - accuracy):.4f}")

    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel", predictionCol="prediction", metricName="f1")
    f1 = f1_evaluator.evaluate(predictions)
    print(f"   F1 Score: {f1:.4f}")

    # 5. Model summary
    gbt_model = model.stages[2]
    print(f"\n7. GBT Model Summary:")
    print(f"   Number of trees: {gbt_model.getNumTrees}")
    print(f"   Feature importances: {gbt_model.featureImportances}")

    spark.stop()
