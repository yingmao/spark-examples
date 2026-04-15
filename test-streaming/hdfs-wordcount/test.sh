#!/bin/bash
set -euo pipefail

# Configurable parameters
OUTPUT_LIMIT=${1:-20}
RUN_DURATION=${2:-60}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== HDFS Streaming Test: Preparing directories ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f \
    /streaming-test/input/ \
    /streaming-test/output/ \
    /spark-checkpoints/hdfs-streaming/ || true

/usr/local/hadoop/bin/hdfs dfs -mkdir -p /streaming-test/input/

echo "=== Starting data generator in background ==="
python3 ./generate_data.py &
GENERATOR_PID=$!
echo "Data generator started (PID: $GENERATOR_PID)"

# Give generator time to create first batch before Spark starts
sleep 8

echo "=== Submitting HDFS Spark Streaming to YARN (Duration: ${RUN_DURATION}s) ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./hdfs_streaming.py "$RUN_DURATION" || true

echo "=== Stopping data generator ==="
kill "$GENERATOR_PID" 2>/dev/null || true
wait "$GENERATOR_PID" 2>/dev/null || true
echo "Data generator stopped."

echo "=== Streaming completed! Showing top $OUTPUT_LIMIT word counts: ==="
/usr/local/hadoop/bin/hdfs dfs -cat /streaming-test/output/part-* 2>/dev/null | \
    grep -v "^word,count$" | sort -t, -k2 -nr | head -n "$OUTPUT_LIMIT" || true

echo "..."
echo "=== Commands for more analysis: ==="
echo "  Full results:     /usr/local/hadoop/bin/hdfs dfs -cat /streaming-test/output/part-*"
echo "  Custom test:      ./test.sh 30 120   (30 lines output, 120s duration)"
echo "  Input files:      /usr/local/hadoop/bin/hdfs dfs -ls /streaming-test/input/"
echo "  YARN UI:          http://$(hostname -I | awk '{print $1}'):8088"
