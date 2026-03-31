#!/bin/bash
set -euo pipefail

OUTPUT_LIMIT=${1:-20}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Sort Test: Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /sort/input/ /sort/output/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /sort/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/sort.txt /sort/input/

echo "=== Submitting PySpark Sort to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./sort.py hdfs:///sort/input/

echo "=== Job completed! Showing first $OUTPUT_LIMIT sorted results: ==="
/usr/local/hadoop/bin/hdfs dfs -cat /sort/output/part-* | head -n "$OUTPUT_LIMIT" || true
echo "..."
echo "=== Commands for more analysis: ==="
echo "  Full sorted output: /usr/local/hadoop/bin/hdfs dfs -cat /sort/output/part-*"
echo "  Custom limit: ./test.sh 100"
