# linear_svc_df.py
"""
Linear Support Vector Classification using Spark ML Pipeline.
Original was already using pyspark.ml - enhanced with parameterization and evaluation.
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: linear_svc_df.py <input_libsvm_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("LinearSVCDataFrame") \
        .getOrCreate()

    print("=== Linear SVC with Spark ML DataFrame API ===\n")

    # 1. Load data
    data = spark.read.format("libsvm").load(sys.argv[1])
    print("1. Input data:")
    data.show(10, truncate=False)
    print(f"   Total records: {data.count()}")

    print("\n2. Label distribution:")
    data.groupBy("label").count().orderBy("label").show()

    # 2. Split data
    (training, test) = data.randomSplit([0.8, 0.2], seed=42)
    print(f"\n3. Data split - Training: {training.count()}, Test: {test.count()}")

    # 3. Configure and train model
    lsvc = LinearSVC(maxIter=10, regParam=0.1)
    print("\n4. Training Linear SVC...")
    lsvcModel = lsvc.fit(training)

    # 4. Model coefficients (same as original)
    print("\n5. Model parameters:")
    print(f"   Coefficients: {lsvcModel.coefficients}")
    print(f"   Intercept: {lsvcModel.intercept}")
    print(f"   Number of features: {lsvcModel.numFeatures}")

    # 5. Evaluation on test data
    predictions = lsvcModel.transform(test)
    print("\n6. Sample predictions:")
    predictions.select("label", "prediction", "rawPrediction").show(5, truncate=False)

    # Evaluation metrics
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = accuracy_evaluator.evaluate(predictions)
    print(f"\n7. Model evaluation:")
    print(f"   Accuracy: {accuracy:.4f}")

    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = auc_evaluator.evaluate(predictions)
    print(f"   AUC-ROC: {auc:.4f}")

    spark.stop()
