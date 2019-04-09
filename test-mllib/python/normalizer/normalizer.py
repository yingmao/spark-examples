from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.feature import Normalizer
from pyspark.mllib.util import MLUtils
import sys
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="NormalizerExample")  # SparkContext

    # $example on$
    data = MLUtils.loadLibSVMFile(sc,sys.argv[1])
    labels = data.map(lambda x: x.label)
    features = data.map(lambda x: x.features)

    normalizer1 = Normalizer()
    normalizer2 = Normalizer(p=float("inf"))

    # Each sample in data1 will be normalized using $L^2$ norm.
    data1 = labels.zip(normalizer1.transform(features))

    # Each sample in data2 will be normalized using $L^\infty$ norm.
    data2 = labels.zip(normalizer2.transform(features))
    # $example off$

    print("data1:")
    for each in data1.collect():
        print(each)

    print("data2:")
    for each in data2.collect():
        print(each)

    sc.stop()
