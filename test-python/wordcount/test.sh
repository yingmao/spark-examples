#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /wordcount/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./wordcount.py hdfs://$SPARK_MASTER:9000/wordcount/input/
