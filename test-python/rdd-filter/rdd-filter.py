from __future__ import print_function
import sys
from pyspark import SparkConf,SparkContext
import random


# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-FILTER")
    
    sc = SparkContext(conf=conf)
    list = []
    for i in range(0,100):
        list.append(random.randint(0,1000))
    x = sc.parallelize(list) 
    y = x.filter(lambda x: x%2 == 1)
    print(x.collect())
    print(y.collect())
