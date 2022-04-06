from __future__ import print_function
import sys
from pyspark import SparkConf,SparkContext
import random

# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-DISTINCT")
    
    sc = SparkContext(conf=conf)
    list = []
    for i in range(0,100):
        list.append(random.randint(0,10))
    x = sc.parallelize(list) 
    y = x.distinct()
    print(x.collect())
    print(y.collect())
