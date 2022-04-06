from __future__ import print_function
import sys
from pyspark import SparkConf,SparkContext
import random

# reload(sys) 
# sys.setdefaultencoding('utf8')

if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-MAXMIN")
    
    sc = SparkContext(conf=conf)
    list = []
    for i in range(0,100):
        list.append(random.randint(0,1000))
    x = sc.parallelize(list) 
    min = x.min()
    max = x.max()
    print(x.collect())
    print("Min:" + str(min))
    print("Max:" + str(max))
