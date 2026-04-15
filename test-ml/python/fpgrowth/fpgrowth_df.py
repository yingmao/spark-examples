# fpgrowth_df.py
"""
FP-Growth Frequent Pattern Mining using Spark ML DataFrame API.
Replaces: pyspark.mllib.fpm.FPGrowth (RDD-based, SparkContext)
With:     pyspark.ml.fpm.FPGrowth    (DataFrame-based, SparkSession)
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.functions import split, col

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: fpgrowth_df.py <input_path>")
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("FPGrowthDataFrame") \
        .getOrCreate()

    print("=== FP-Growth with Spark ML DataFrame API ===\n")

    # 1. Load and transform data (replaces sc.textFile + RDD map)
    raw_df = spark.read.text(sys.argv[1])
    transactions_df = raw_df.select(
        split(col("value"), " ").alias("items")
    )

    print("1. Transaction data:")
    transactions_df.show(10, truncate=False)
    print(f"   Total transactions: {transactions_df.count()}")

    # 2. Train FP-Growth model (replaces FPGrowth.train(rdd))
    fpgrowth = FPGrowth(
        itemsCol="items",
        minSupport=0.2,
        minConfidence=0.6
    )
    model = fpgrowth.fit(transactions_df)

    # 3. Results (replaces manual collect() loop)
    print("\n2. Frequent Itemsets:")
    model.freqItemsets.orderBy(col("freq").desc()).show(truncate=False)

    print("3. Association Rules (new capability):")
    model.associationRules.orderBy(col("confidence").desc()).show(truncate=False)

    # 4. Transform new transactions
    print("4. Predictions for sample transactions:")
    sample_transactions = spark.createDataFrame([
        (["milk", "bread"],),
        (["bread", "butter"],)
    ], ["items"])
    
    predictions = model.transform(sample_transactions)
    predictions.show(truncate=False)

    spark.stop()
