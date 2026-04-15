#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating FP-Growth transaction data ==="
cat > transactions.txt << 'EOF'
milk bread butter
bread butter jam
milk bread
milk bread butter eggs
bread jam
milk cookies
cookies juice
milk cookies bread
butter jam bread
milk bread butter
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /ml_examples/fpgrowth/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ml_examples/fpgrowth/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal transactions.txt /ml_examples/fpgrowth/input/

echo "=== Submitting FP-Growth DataFrame Example ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./fpgrowth_df.py \
    hdfs:///ml_examples/fpgrowth/input/transactions.txt

echo "=== FP-Growth example completed! ==="
