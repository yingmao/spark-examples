# logistic_regression_df.py
"""
Logistic Regression using Spark ML Pipeline with comprehensive parameter management.
Original was already using pyspark.ml - enhanced with proper structure and evaluation.
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("LogisticRegressionDF") \
        .getOrCreate()

    print("=== Logistic Regression with Spark ML Parameter Management ===\n")

    # 1. Prepare training data (enhanced from original)
    training = spark.createDataFrame([
        (1.0, Vectors.dense([0.0, 1.1,  0.1])),
        (0.0, Vectors.dense([2.0, 1.0, -1.0])),
        (0.0, Vectors.dense([2.0, 1.3,  1.0])),
        (1.0, Vectors.dense([0.0, 1.2, -0.5])),
        (1.0, Vectors.dense([0.1, 0.9,  0.2])),
        (0.0, Vectors.dense([1.9, 1.1, -0.8]))
    ], ["label", "features"])

    print("1. Training data:")
    training.show()

    # 2. Create LogisticRegression Estimator
    lr = LogisticRegression(maxIter=10, regParam=0.01)
    print("2. LogisticRegression parameters:")
    print(lr.explainParams() + "\n")

    # 3. Model 1: Default parameters
    model1 = lr.fit(training)
    print("3. Model 1 parameters:")
    print(model1.extractParamMap())

    # 4. Model 2: Custom parameter map (same as original)
    paramMap = {lr.maxIter: 20}
    paramMap[lr.maxIter] = 30  # Overwrite
    paramMap.update({lr.regParam: 0.1, lr.threshold: 0.55})
    
    paramMap2 = {lr.probabilityCol: "myProbability"}
    paramMapCombined = paramMap.copy()
    paramMapCombined.update(paramMap2)

    model2 = lr.fit(training, paramMapCombined)
    print("\n4. Model 2 parameters:")
    print(model2.extractParamMap())

    # 5. Test data and predictions
    test = spark.createDataFrame([
        (1.0, Vectors.dense([-1.0, 1.5,  1.3])),
        (0.0, Vectors.dense([ 3.0, 2.0, -0.1])),
        (1.0, Vectors.dense([ 0.0, 2.2, -1.5]))
    ], ["label", "features"])

    prediction = model2.transform(test)
    result = prediction.select("features", "label", "myProbability", "prediction").collect()

    print("\n5. Predictions:")
    for row in result:
        print("features=%s, label=%s -> prob=%s, prediction=%s"
              % (row.features, row.label, row.myProbability, row.prediction))

    # 6. Evaluation metrics
    evaluator = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(prediction)
    print(f"\n6. Model evaluation - AUC: {auc:.4f}")

    spark.stop()
