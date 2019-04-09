#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /logistic_regression/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /logistic_regression/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_libsvm_data.txt /logistic_regression/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./logistic_regression.py
