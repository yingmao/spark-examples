from __future__ import print_function
import sys
from pyspark import SparkConf,SparkContext
import random

# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-FLATMAP")
    
    sc = SparkContext(conf=conf)
    list = []
    for i in range(0,100):
        list.append(random.randint(0,1000))
    x = sc.parallelize(list) 
    y = x.flatMap(lambda x: (x, 100*x, x**2))
    print(x.collect())
    print(y.collect())
