from __future__ import print_function

# $example on$
from numpy import array
# $example off$
import sys
from pyspark import SparkContext
# $example on$
from pyspark.mllib.clustering import BisectingKMeans
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PythonBisectingKMeansExample")  # SparkContext

    # $example on$
    # Load and parse the data
    data = sc.textFile(sys.argv[1])
    parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

    # Build the model (cluster the data)
    model = BisectingKMeans.train(parsedData, 2, maxIterations=5)

    # Evaluate clustering
    cost = model.computeCost(parsedData)
    print("Bisecting K-means Cost = " + str(cost))
    # $example off$

    sc.stop()
