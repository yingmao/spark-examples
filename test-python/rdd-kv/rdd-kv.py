from __future__ import print_function
from pyspark.sql.functions import col
import sys
from operator import add
from pyspark import SparkConf,SparkContext
from pyspark.sql import *

# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    conf = SparkConf().setAppName("RDD-KV")
    
    sc = SparkContext(conf=conf)
    kvRdd1 = sc.parallelize([(1, 4),(2, 5),(3, 6),(4, 7)])
    keys = kvRdd1.keys().collect()
    print("Keys==>")
    for item in keys:
        print(item)
    values = kvRdd1.values().collect()
    print("Values===>")
    for item in values:
        print(values)
    result = (kvRdd1.filter(lambda keyValue: keyValue[0] > 2)).collect()
    print("Filter===>")
    for item in result:
        print(item)
    
