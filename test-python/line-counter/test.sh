#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /line-counter/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /line-counter/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /line-counter/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /line-counter/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./counter.py hdfs://$SPARK_MASTER:9000/line-counter/input/
