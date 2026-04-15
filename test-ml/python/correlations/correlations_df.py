# correlations_df.py
"""
Statistical correlations using Spark DataFrame API.
Replaces: pyspark.mllib.stat.Statistics (RDD-based)
With: DataFrame.stat.corr() and pyspark.ml.stat.Correlation (DataFrame-based)
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CorrelationsDataFrame") \
        .getOrCreate()

    print("=== Correlations with Spark DataFrame API ===\n")

    # -------------------------------------------------------
    # Part 1: Simple pairwise correlation (replaces Statistics.corr(seriesX, seriesY))
    # -------------------------------------------------------
    print("1. Simple pairwise correlations:")
    print("   Replaces: Statistics.corr(seriesX, seriesY)")

    # Create DataFrame instead of separate RDDs
    data_simple = [
        (1.0, 11.0),
        (2.0, 22.0), 
        (3.0, 33.0),
        (3.0, 33.0),
        (5.0, 555.0)
    ]
    df_simple = spark.createDataFrame(data_simple, ["x", "y"])
    df_simple.show()

    # DataFrame correlation methods (replaces Statistics.corr())
    pearson_corr = df_simple.stat.corr("x", "y", method="pearson")
    spearman_corr = df_simple.stat.corr("x", "y", method="spearman") 
    
    print(f"   Pearson correlation:  {pearson_corr:.6f}")
    print(f"   Spearman correlation: {spearman_corr:.6f}")

    # -------------------------------------------------------
    # Part 2: Correlation matrix (replaces Statistics.corr(rdd_of_vectors))
    # -------------------------------------------------------
    print("\n2. Multi-dimensional correlation matrix:")
    print("   Replaces: Statistics.corr(rdd_of_vectors)")

    # Create DataFrame with Vector column (same data as original)
    data_vectors = [
        (Vectors.dense([1.0, 10.0, 100.0]),),
        (Vectors.dense([2.0, 20.0, 200.0]),),
        (Vectors.dense([5.0, 33.0, 366.0]),)
    ]
    df_vectors = spark.createDataFrame(data_vectors, ["features"])
    df_vectors.show(truncate=False)

    # ML stat correlation (replaces Statistics.corr(rdd, method))
    pearson_matrix = Correlation.corr(df_vectors, "features", method="pearson")
    spearman_matrix = Correlation.corr(df_vectors, "features", method="spearman")

    print("   Pearson correlation matrix:")
    print(pearson_matrix.collect()[0][0])
    
    print("\n   Spearman correlation matrix:")  
    print(spearman_matrix.collect()[0][0])

    # -------------------------------------------------------
    # Part 3: Demonstrate DataFrame advantages
    # -------------------------------------------------------
    print("\n3. DataFrame advantages over RDD approach:")
    
    # Easy aggregations and statistics
    print("   Easy descriptive statistics:")
    df_simple.describe().show()
    
    # SQL-like operations
    df_simple.createOrReplaceTempView("correlations_data")
    result = spark.sql("""
        SELECT 
            COUNT(*) as count,
            AVG(x) as avg_x, 
            AVG(y) as avg_y,
            STDDEV(x) as std_x,
            STDDEV(y) as std_y
        FROM correlations_data
    """)
    print("   SQL aggregations:")
    result.show()

    spark.stop()
