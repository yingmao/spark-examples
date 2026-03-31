#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== WordCount Test: Cleaning HDFS directories ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /wordcount/input/ /wordcount/output/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /wordcount/input/

echo "=== Uploading test data to HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /wordcount/input/

echo "=== Submitting PySpark WordCount to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./wordcount.py hdfs:///wordcount/input/

echo "=== Job completed! Showing first 20 results: ==="
# The '|| true' prevents script failure due to SIGPIPE when head closes the pipe early
/usr/local/hadoop/bin/hdfs dfs -cat /wordcount/output/part-* | head -n 20 || true

echo "..."
echo "=== Output truncated. To see full results: ==="
echo "  /usr/local/hadoop/bin/hdfs dfs -cat /wordcount/output/part-*"
