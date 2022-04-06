from __future__ import print_function
from pyspark.sql.functions import col
import sys
from operator import add
from pyspark import SparkConf,SparkContext
from pyspark.sql import *

# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: counter.py <file>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("TextLineCounter")
    
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    textFile = sc.textFile(sys.argv[1])
    df = textFile.map(lambda r: Row(r)).toDF(["line"])
    print("line count:" + str(df.count()))
