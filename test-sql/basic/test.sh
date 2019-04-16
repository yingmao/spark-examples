#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /sql_basic/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /sql_basic/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/people.json /sql_basic
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/people.txt /sql_basic
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./basic.py
