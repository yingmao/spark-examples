#!/bin/sh
set -euo pipefail

HADOOP_HOME="/usr/local/hadoop"
SPARK_HOME="/usr/local/spark"

echo "========================================"
echo "    Stopping Hadoop Cluster (YARN)     "
echo "========================================"

echo "=== Stopping Spark History Server ==="
"$SPARK_HOME/sbin/stop-history-server.sh" || echo "Spark History Server may not be running"

echo "=== Stopping YARN (Resource Manager) ==="
"$HADOOP_HOME/sbin/stop-yarn.sh" || echo "YARN services may not be running"

echo "=== Stopping HDFS (Distributed Storage) ==="
"$HADOOP_HOME/sbin/stop-dfs.sh" || echo "HDFS services may not be running"

echo "========================================"
echo "         Verifying Shutdown Status      "
echo "========================================"

echo "=== Remaining Java Processes ==="
REMAINING_PROCS=$(jps | grep -E "(NameNode|DataNode|ResourceManager|NodeManager|HistoryServer)" || true)
if [ -n "$REMAINING_PROCS" ]; then
    echo "Warning: Some Hadoop/Spark processes are still running:"
    echo "$REMAINING_PROCS"
else
    echo "✓ All Hadoop/Spark services stopped successfully"
fi

echo "========================================"
echo "        Cluster Stopped Successfully    "
echo "========================================"
