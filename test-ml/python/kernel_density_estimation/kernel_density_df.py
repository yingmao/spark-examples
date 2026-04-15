# kernel_density_df.py
"""
Kernel Density Estimation using SparkSession entry point.
Note: KDE is only available in pyspark.mllib, so we access it via spark.sparkContext
while maintaining the modern SparkSession entry point.
"""

from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.mllib.stat import KernelDensity

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("KernelDensityEstimationDF") \
        .getOrCreate()

    print("=== Kernel Density Estimation with SparkSession ===\n")

    # Access SparkContext through SparkSession (modern approach)
    sc = spark.sparkContext

    # Same data as original example
    data = sc.parallelize([1.0, 1.0, 1.0, 2.0, 3.0, 4.0, 5.0, 5.0, 6.0, 7.0, 8.0, 9.0, 9.0])

    print("1. Sample data points:")
    print(f"   Data: {data.collect()}")
    print(f"   Count: {data.count()}")

    # Construct density estimator (same as original)
    kd = KernelDensity()
    kd.setSample(data)
    kd.setBandwidth(3.0)

    # Estimate densities at query points
    query_points = [-1.0, 2.0, 5.0]
    densities = kd.estimate(query_points)

    print(f"\n2. Kernel Density Estimation:")
    print(f"   Bandwidth: 3.0")
    print(f"   Query points: {query_points}")
    print(f"   Densities: {list(densities)}")

    # Additional analysis using DataFrame (bonus)
    print("\n3. Additional analysis using DataFrame:")
    sample_df = spark.createDataFrame([(float(x),) for x in data.collect()], ["value"])
    sample_df.describe().show()

    spark.stop()
