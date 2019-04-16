#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /hdfs_wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /hdfs_wordcount/input/
python ./hdfs_wordcount.py hdfs://$SPARK_MASTER:9000/hdfs_wordcount/input/
