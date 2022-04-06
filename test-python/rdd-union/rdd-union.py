from __future__ import print_function
import sys
from pyspark import SparkConf,SparkContext
import random

# reload(sys) 
# sys.setdefaultencoding('utf8')

if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-UNION")
    
    sc = SparkContext(conf=conf)
    list1 = []
    for i in range(0,10):
        list1.append(random.randint(0,100))
    list2 = []
    for i in range(0,10):
        list2.append(random.randint(0,100))
    x = sc.parallelize(list1) 
    y = sc.parallelize(list2)
    z = x.union(y)
    print(x.collect())
    print(y.collect())
    print(z.collect())
