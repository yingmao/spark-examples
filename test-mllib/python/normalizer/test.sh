#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /normalizer/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /normalizer/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_libsvm_data.txt /normalizer/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./normalizer.py hdfs://$SPARK_MASTER:9000/normalizer/input/
