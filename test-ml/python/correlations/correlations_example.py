from __future__ import print_function

import numpy as np

from pyspark import SparkContext
# $example on$
from pyspark.mllib.stat import Statistics
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="CorrelationsExample")  # SparkContext

    # $example on$
    seriesX = sc.parallelize([1.0, 2.0, 3.0, 3.0, 5.0])  # a series
    # seriesY must have the same number of partitions and cardinality as seriesX
    seriesY = sc.parallelize([11.0, 22.0, 33.0, 33.0, 555.0])

    # Compute the correlation using Pearson's method. Enter "spearman" for Spearman's method.
    # If a method is not specified, Pearson's method will be used by default.
    print("Correlation is: " + str(Statistics.corr(seriesX, seriesY, method="pearson")))

    data = sc.parallelize(
        [np.array([1.0, 10.0, 100.0]), np.array([2.0, 20.0, 200.0]), np.array([5.0, 33.0, 366.0])]
    )  # an RDD of Vectors

    # calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    # If a method is not specified, Pearson's method will be used by default.
    print(Statistics.corr(data, method="pearson"))
    # $example off$

    sc.stop()
