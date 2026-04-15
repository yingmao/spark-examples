#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /standard_scaler/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /standard_scaler/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_libsvm_data.txt /standard_scaler/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./standard_scaler.py hdfs://$SPARK_MASTER:9000/standard_scaler/input/
