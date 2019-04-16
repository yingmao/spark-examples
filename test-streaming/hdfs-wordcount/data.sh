#!/bin/bash
source ../../env.sh
/usr/local/hadoop/bin/hdfs dfs -put ../../test-data/test.txt /hdfs_wordcount/input/$RANDOM.txt
sleep 5
killall python
