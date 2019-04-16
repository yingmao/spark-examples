#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /hive/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./hive.py hdfs://$SPARK_MASTER:9000/hive/input/
rm -rf metastore_db
rm -rf *.log
