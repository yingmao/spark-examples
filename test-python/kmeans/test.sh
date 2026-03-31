#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== KMeans Test: Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /kmeans/input/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /kmeans/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/kmeans.txt /kmeans/input/

echo "=== Submitting PySpark KMeans to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./kmeans.py hdfs:///kmeans/input/

echo "=== KMeans clustering completed! ==="
