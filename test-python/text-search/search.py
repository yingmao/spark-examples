from __future__ import print_function
from pyspark.sql.functions import col
import sys
from operator import add
from pyspark import SparkConf,SparkContext
from pyspark.sql import *

# reload(sys) 
# sys.setdefaultencoding('utf8')
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: search.py <file> <keyword>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf().setAppName("TextSearch")
    
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    textFile = sc.textFile(sys.argv[1])
    df = textFile.map(lambda r: Row(r)).toDF(["line"])
    res = df.filter(col("line").like("%" + sys.argv[2] + "%"))
    #res.count()
    data = res.collect()
    for item in data:
        print(item)
