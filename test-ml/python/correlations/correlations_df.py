# correlations_df.py
"""
Statistical correlations using Spark DataFrame API.
Correctly handles both Pearson and Spearman methods.
"""

from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("CorrelationsDataFrame") \
        .getOrCreate()

    # Set log level to ERROR to see our output clearly
    spark.sparkContext.setLogLevel("ERROR")

    print("\n=== Correlations with Spark DataFrame API ===\n")

    # -------------------------------------------------------
    # Part 1: Simple pairwise correlation
    # -------------------------------------------------------
    print("1. Simple pairwise correlations:")
    
    data_simple = [
        (1.0, 11.0),
        (2.0, 22.0), 
        (3.0, 33.0),
        (3.0, 33.0),
        (5.0, 555.0)
    ]
    df_simple = spark.createDataFrame(data_simple, ["x", "y"])
    df_simple.show()

    # Pearson: Works directly on columns
    pearson_corr = df_simple.stat.corr("x", "y", method="pearson")
    print(f"   Pearson correlation (Direct): {pearson_corr:.6f}")

    # Spearman: Requires VectorAssembler + Correlation.corr
    # We bundle 'x' and 'y' into a single vector column named 'features'
    assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
    df_vector_simple = assembler.transform(df_simple)

    # Correlation.corr returns a DataFrame with a single row containing the matrix
    spearman_matrix = Correlation.corr(df_vector_simple, "features", method="spearman").collect()[0][0]
    
    # Extract the value from the 2x2 matrix at position (0, 1)
    spearman_corr = float(spearman_matrix[0, 1])
    print(f"   Spearman correlation (MLlib): {spearman_corr:.6f}")

    # -------------------------------------------------------
    # Part 2: Multi-dimensional correlation matrix
    # -------------------------------------------------------
    print("\n2. Multi-dimensional correlation matrix:")

    data_vectors = [
        (Vectors.dense([1.0, 10.0, 100.0]),),
        (Vectors.dense([2.0, 20.0, 200.0]),),
        (Vectors.dense([5.0, 33.0, 366.0]),)
    ]
    df_vectors = spark.createDataFrame(data_vectors, ["features"])
    df_vectors.show(truncate=False)

    # Calculate matrices
    p_matrix = Correlation.corr(df_vectors, "features", method="pearson").collect()[0][0]
    s_matrix = Correlation.corr(df_vectors, "features", method="spearman").collect()[0][0]

    print("   Pearson correlation matrix:")
    print(p_matrix)
    
    print("\n   Spearman correlation matrix:")  
    print(s_matrix)

    # -------------------------------------------------------
    # Part 3: Demonstrate DataFrame advantages
    # -------------------------------------------------------
    print("\n3. DataFrame advantages over RDD approach:")
    
    print("   Easy descriptive statistics:")
    df_simple.describe().show()
    
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