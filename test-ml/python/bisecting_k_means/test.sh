#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating Bisecting K-Means test data ==="
# Generate LIBSVM format data with 2 clear clusters
python3 - << 'EOF'
import random
random.seed(42)

lines = []
# Cluster 1: centered around (1, 1)
for _ in range(50):
    x1 = 1.0 + random.gauss(0, 0.3)
    x2 = 1.0 + random.gauss(0, 0.3)
    lines.append(f"0 1:{x1:.4f} 2:{x2:.4f}")

# Cluster 2: centered around (5, 5)  
for _ in range(50):
    x1 = 5.0 + random.gauss(0, 0.3)
    x2 = 5.0 + random.gauss(0, 0.3)
    lines.append(f"1 1:{x1:.4f} 2:{x2:.4f}")

random.shuffle(lines)
with open("bisecting_kmeans_data.txt", "w") as f:
    f.write("\n".join(lines))
    
print(f"Generated bisecting_kmeans_data.txt ({len(lines)} points)")
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /ml_examples/bisecting_kmeans/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ml_examples/bisecting_kmeans/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal bisecting_kmeans_data.txt /ml_examples/bisecting_kmeans/input/

echo "=== Submitting Bisecting K-Means to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./bisecting_kmeans_df.py \
    hdfs:///ml_examples/bisecting_kmeans/input/bisecting_kmeans_data.txt

echo "=== Bisecting K-Means DataFrame example completed! ==="
