#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Submitting Kernel Density Estimation ==="
echo "Note: Uses in-memory data, no HDFS input required"

/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./kernel_density_df.py

echo "=== Kernel Density Estimation completed! ==="
