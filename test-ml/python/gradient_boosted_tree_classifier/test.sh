#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating GBT classification test data ==="
python3 - << 'EOF'
import random
random.seed(42)

lines = []
# Class 0: small feature values
for _ in range(80):
    f1 = random.uniform(0.0, 2.0)
    f2 = random.uniform(0.0, 2.0)
    f3 = random.uniform(0.0, 2.0)
    lines.append(f"0.0 1:{f1:.4f} 2:{f2:.4f} 3:{f3:.4f}")

# Class 1: large feature values
for _ in range(80):
    f1 = random.uniform(4.0, 6.0)
    f2 = random.uniform(4.0, 6.0)
    f3 = random.uniform(4.0, 6.0)
    lines.append(f"1.0 1:{f1:.4f} 2:{f2:.4f} 3:{f3:.4f}")

random.shuffle(lines)
with open("gbt_data.txt", "w") as f:
    f.write("\n".join(lines))

print(f"Generated gbt_data.txt ({len(lines)} samples)")
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /ml_examples/gbt/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ml_examples/gbt/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal gbt_data.txt /ml_examples/gbt/input/

echo "=== Submitting GBT Classification ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./gbt_classification_df.py \
    hdfs:///ml_examples/gbt/input/gbt_data.txt

echo "=== GBT Classification completed! ==="
