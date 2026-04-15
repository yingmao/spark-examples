#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /user/root/data/mllib/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /user/root/data/mllib/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_libsvm_data.txt /user/root/data/mllib/sample_libsvm_data.txt
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./linearsvc.py
