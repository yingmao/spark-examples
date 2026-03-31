#!/bin/bash
set -euo pipefail

OUTPUT_LIMIT=${1:-20}
ITERATIONS=${2:-5}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== PageRank Test: Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /pagerank/input/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /pagerank/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/pagerank.txt /pagerank/input/

echo "=== Submitting PySpark PageRank to YARN (Iterations: $ITERATIONS) ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./pagerank.py hdfs:///pagerank/input/ "$ITERATIONS"

echo "=== PageRank completed! Results available in application output ==="
echo "=== Usage: ./test.sh 30 10  (30 lines output, 10 iterations) ==="
