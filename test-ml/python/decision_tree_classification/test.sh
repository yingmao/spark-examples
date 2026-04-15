#!/bin/bash
set -euo pipefail

echo "=== Cleaning any previous YARN applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating Decision Tree test data ==="
# Generate LIBSVM format classification data
python3 - << 'EOF'
import random
random.seed(42)

lines = []
# Class 0: small feature values
for _ in range(40):
    f1 = random.uniform(0, 2)
    f2 = random.uniform(0, 2)  
    f3 = random.uniform(0, 2)
    lines.append(f"0.0 1:{f1:.4f} 2:{f2:.4f} 3:{f3:.4f}")

# Class 1: large feature values
for _ in range(40):
    f1 = random.uniform(4, 6)
    f2 = random.uniform(4, 6)
    f3 = random.uniform(4, 6) 
    lines.append(f"1.0 1:{f1:.4f} 2:{f2:.4f} 3:{f3:.4f}")

random.shuffle(lines)
with open("decision_tree_data.txt", "w") as f:
    f.write("\n".join(lines))
    
print(f"Generated decision_tree_data.txt ({len(lines)} samples)")
print("Class 0: 40 samples with small feature values (0-2)")
print("Class 1: 40 samples with large feature values (4-6)")
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /ml_examples/decision_tree/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /ml_examples/decision_tree/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal decision_tree_data.txt /ml_examples/decision_tree/input/

echo "=== Submitting Decision Tree Classification to YARN ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 512m \
    --executor-memory 512m \
    ./decision_tree_classification_df.py \
    hdfs:///ml_examples/decision_tree/input/decision_tree_data.txt

echo "=== Decision Tree Classification completed! ==="
