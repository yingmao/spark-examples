#!/bin/bash
source ../../env.sh
mvn clean package
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -rm -r /wordcount/output
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /wordcount/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /wordcount/input/
/usr/local/spark/bin/spark-submit --class WordCount --master=spark://$SPARK_MASTER:7077 target/WordCount-1.0-SNAPSHOT.jar hdfs://$SPARK_MASTER:9000/wordcount/input/ hdfs://$SPARK_MASTER:9000/wordcount/output
/usr/local/hadoop/bin/hdfs dfs -cat /wordcount/output/part-00000
mvn clean
