#!/bin/bash
source ../../../env.sh
mvn clean package
/usr/local/hadoop/bin/hdfs dfs -rm -r /fpgrowth/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /fpgrowth/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../../test-data/fpgrowth.txt /fpgrowth/input/
/usr/local/spark/bin/spark-submit --class FPGrowthExample --master=spark://$SPARK_MASTER:7077 target/FPGrowth-1.0-SNAPSHOT.jar --minSupport 0.8 --numPartition 2 hdfs://$SPARK_MASTER:9000/fpgrowth/input/
mvn clean
