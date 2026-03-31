#!/bin/bash
set -euo pipefail

PARTITIONS=${1:-10}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Submitting PySpark Pi to YARN (Partitions: $PARTITIONS) ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./pi.py "$PARTITIONS"

echo "=== Pi estimation completed! ==="
echo "=== Usage: ./test.sh 100  (use 100 partitions for higher accuracy) ==="
