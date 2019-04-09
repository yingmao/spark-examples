#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /fpgrowth/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /fpgrowth/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /fpgrowth/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/fpgrowth.txt /fpgrowth/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./fpgrowth.py hdfs://$SPARK_MASTER:9000/fpgrowth/input/
