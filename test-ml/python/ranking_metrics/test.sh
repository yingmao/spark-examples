#!/bin/bash
source ../../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /ranking_metrics/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ranking_metrics/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/sample_movielens_data.txt /ranking_metrics/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./ranking_metrics.py hdfs://$SPARK_MASTER:9000/ranking_metrics/input/
