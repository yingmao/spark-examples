#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /decision_tree_classification/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /decision_tree_classification/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_libsvm_data.txt /decision_tree_classification/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./decision_tree_classification.py
