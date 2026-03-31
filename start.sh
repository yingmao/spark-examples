#!/bin/sh
set -euo pipefail

HADOOP_HOME="/usr/local/hadoop"
SPARK_HOME="/usr/local/spark"

echo "========================================"
echo "    Starting Hadoop Cluster (YARN)     "
echo "========================================"

echo "=== Starting HDFS (Distributed Storage) ==="
"$HADOOP_HOME/sbin/start-dfs.sh"

echo "=== Starting YARN (Resource Manager) ==="
"$HADOOP_HOME/sbin/start-yarn.sh"

echo "=== Starting Spark History Server ==="
"$SPARK_HOME/sbin/start-history-server.sh" || echo "Warning: Spark History Server failed to start"

# Wait for services to initialize
sleep 5

echo "========================================"
echo "         Verifying Cluster Status       "
echo "========================================"

echo "=== HDFS Status ==="
# FIX: Quote only the executable path, not the arguments
"$HADOOP_HOME/bin/hdfs" dfsadmin -report | head -20 || echo "HDFS verification failed"

echo
echo "=== YARN Nodes ==="
# FIX: Quote only the executable path, not the arguments
"$HADOOP_HOME/bin/yarn" node -list || echo "YARN verification failed"

echo
echo "=== Java Processes on Manager ==="
jps | grep -E "(NameNode|ResourceManager|HistoryServer|DataNode|NodeManager)" || echo "Some expected processes may be missing"

# Get manager IP for web interface URLs
MANAGER_IP=$(hostname -I | awk '{print $1}' | head -1)

echo "========================================"
echo "        Cluster Started Successfully     "
echo "========================================"
echo "Web Interfaces:"
echo "  HDFS NameNode:        http://${MANAGER_IP}:9870"
echo "  YARN ResourceManager: http://${MANAGER_IP}:8088"
echo "  Spark History Server: http://${MANAGER_IP}:18080"
echo ""
echo "Test your cluster:"
echo "  /usr/local/spark/bin/pyspark --master yarn"
echo "========================================"
