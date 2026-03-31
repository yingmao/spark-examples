#!/bin/bash
set -euo pipefail

OUTPUT_LIMIT=${1:-20}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Line Counter Test: Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /line-counter/input/ /line-counter/output/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /line-counter/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /line-counter/input/

echo "=== Submitting PySpark Line Counter to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./counter.py hdfs:///line-counter/input/

echo "=== Job completed! Showing first $OUTPUT_LIMIT results: ==="
/usr/local/hadoop/bin/hdfs dfs -cat /line-counter/output/part-* | head -n "$OUTPUT_LIMIT" || true
echo "..."
echo "=== Commands for more analysis: ==="
echo "  Full results: /usr/local/hadoop/bin/hdfs dfs -cat /line-counter/output/part-*"
echo "  Custom limit: ./test.sh 50"
