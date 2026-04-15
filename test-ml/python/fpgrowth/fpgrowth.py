from pyspark.mllib.fpm import FPGrowth
# $example off$
from pyspark import SparkContext
import sys

if __name__ == "__main__":
    sc = SparkContext(appName="FPGrowth")

    # $example on$
    data = sc.textFile(sys.argv[1])
    transactions = data.map(lambda line: line.strip().split(' '))
    model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
    result = model.freqItemsets().collect()
    for fi in result:
        print(fi)
