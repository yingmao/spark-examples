#!/bin/bash
source ../../env.sh
mvn clean package
/usr/local/hadoop/bin/hdfs dfs -rm -r /pagerank/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /pagerank/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/pagerank.txt /pagerank/input/
/usr/local/spark/bin/spark-submit --class SparkPageRank --master=spark://$SPARK_MASTER:7077 target/PageRank-1.0-SNAPSHOT.jar hdfs://$SPARK_MASTER:9000/pagerank/input/
mvn clean
