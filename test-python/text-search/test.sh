#!/bin/bash
set -euo pipefail

OUTPUT_LIMIT=${1:-20}
SEARCH_TERM=${2:-"new"}

echo "=== Cleaning any previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Text Search Test: Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /text-search/input/ /text-search/output/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /text-search/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../../test-data/test.txt /text-search/input/

echo "=== Submitting PySpark Text Search to YARN (Searching for: '$SEARCH_TERM') ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./search.py hdfs:///text-search/input/ "$SEARCH_TERM"

echo "=== Job completed! Showing first $OUTPUT_LIMIT matching lines: ==="
/usr/local/hadoop/bin/hdfs dfs -cat /text-search/output/part-* | head -n "$OUTPUT_LIMIT" || true
echo "..."
echo "=== Commands for more analysis: ==="
echo "  Full search results: /usr/local/hadoop/bin/hdfs dfs -cat /text-search/output/part-*"
echo "  Custom search: ./test.sh 50 error  (50 lines, search 'error')"
