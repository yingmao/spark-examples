#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /text-search/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /text-search/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /text-search/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /text-search/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./search.py hdfs://$SPARK_MASTER:9000/text-search/input/ new
