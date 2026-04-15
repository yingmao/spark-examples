# decision_tree_classification_df.py
"""
Decision Tree Classification using complete Spark ML Pipeline.
Enhanced version with proper parameterization and comprehensive evaluation.
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: decision_tree_classification_df.py <input_libsvm_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("DecisionTreeClassificationDF") \
        .getOrCreate()

    print("=== Decision Tree Classification with ML Pipeline ===\n")

    # 1. Load data in LIBSVM format
    data = spark.read.format("libsvm").load(sys.argv[1])
    
    print("1. Input data:")
    data.show(10, truncate=False)
    print(f"   Total records: {data.count()}")

    # Check label distribution
    print("\n2. Label distribution:")
    data.groupBy("label").count().orderBy("label").show()

    # 3. Split data into training and test sets
    (trainingData, testData) = data.randomSplit([0.7, 0.3], seed=42)
    print(f"\n3. Data split:")
    print(f"   Training: {trainingData.count()} records")
    print(f"   Test:     {testData.count()} records")

    # 4. Build ML Pipeline with 3 stages
    print("\n4. Building ML Pipeline:")
    print("   Stage 1: StringIndexer (label encoding)")
    print("   Stage 2: VectorIndexer (categorical feature detection)")  
    print("   Stage 3: DecisionTreeClassifier (model training)")

    # Stage 1: Index labels (converts string labels to numeric)
    labelIndexer = StringIndexer(
        inputCol="label", 
        outputCol="indexedLabel"
    ).fit(data)

    # Stage 2: Automatically identify categorical features
    featureIndexer = VectorIndexer(
        inputCol="features",
        outputCol="indexedFeatures", 
        maxCategories=4
    ).fit(data)

    # Stage 3: Decision Tree Classifier
    dt = DecisionTreeClassifier(
        labelCol="indexedLabel",
        featuresCol="indexedFeatures",
        maxDepth=5,
        seed=42
    )

    # Create pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

    # 5. Train the pipeline
    print("\n5. Training pipeline...")
    model = pipeline.fit(trainingData)

    # 6. Make predictions
    predictions = model.transform(testData)
    
    print("\n6. Sample predictions:")
    predictions.select("prediction", "indexedLabel", "features").show(5, truncate=False)

    # 7. Evaluate model performance
    print("\n7. Model evaluation:")
    
    # Accuracy
    evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction", 
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print(f"   Accuracy: {accuracy:.4f}")
    print(f"   Test Error: {(1.0 - accuracy):.4f}")

    # F1 Score
    f1_evaluator = MulticlassClassificationEvaluator(
        labelCol="indexedLabel",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = f1_evaluator.evaluate(predictions)
    print(f"   F1 Score: {f1:.4f}")

    # 8. Extract and analyze the trained tree
    treeModel = model.stages[2]
    print(f"\n8. Decision Tree Summary:")
    print(f"   Tree depth: {treeModel.depth}")
    print(f"   Number of nodes: {treeModel.numNodes}")
    print(f"   Feature importances: {treeModel.featureImportances}")

    print(f"\n9. Tree structure:")
    print(treeModel.toDebugString)

    spark.stop()
