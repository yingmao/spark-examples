#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating Linear SVC test data ==="
python3 - << 'EOF'
import random
random.seed(42)

lines = []
# Class 0: linearly separable region
for _ in range(60):
    f1 = random.uniform(-3.0, 0.0)
    f2 = random.uniform(-3.0, 0.0)
    lines.append(f"0.0 1:{f1:.4f} 2:{f2:.4f}")

# Class 1: linearly separable region
for _ in range(60):
    f1 = random.uniform(1.0, 4.0)
    f2 = random.uniform(1.0, 4.0)
    lines.append(f"1.0 1:{f1:.4f} 2:{f2:.4f}")

random.shuffle(lines)
with open("svc_data.txt", "w") as f:
    f.write("\n".join(lines))

print(f"Generated svc_data.txt ({len(lines)} samples)")
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /ml_examples/linear_svc/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ml_examples/linear_svc/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal svc_data.txt /ml_examples/linear_svc/input/

echo "=== Submitting Linear SVC ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./linear_svc_df.py \
    hdfs:///ml_examples/linear_svc/input/svc_data.txt

echo "=== Linear SVC completed! ==="
