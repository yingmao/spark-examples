#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /kmeans/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /kmeans/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/kmeans.txt /kmeans/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./kmeans.py hdfs://$SPARK_MASTER:9000/kmeans/input/
