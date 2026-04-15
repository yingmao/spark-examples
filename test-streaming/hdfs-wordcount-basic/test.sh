#!/bin/bash
set -euo pipefail

echo "🧹 Step 1: Cleaning up previous applications"
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    /usr/local/hadoop/bin/yarn application -kill "$app" >/dev/null 2>&1 || true
done

echo "📁 Step 2: Preparing HDFS directories"
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /streaming-test/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /streaming-test/input

echo "📝 Step 3: Creating sample data files"
python3 ./generate_data.py

echo "📊 Step 4: Listing files in HDFS (students can see what will be processed)"
echo "Files ready for streaming:"
/usr/local/hadoop/bin/hdfs dfs -ls /streaming-test/input

echo ""
echo "🚀 Step 5: Starting Spark Streaming Demo"
echo "=========================================="
echo "INSTRUCTIONS FOR STUDENTS:"
echo "1. Watch the console output below"
echo "2. You'll see 5 batches (Batch 0, 1, 2, 3, 4)"  
echo "3. Notice how word counts increase with each batch"
echo "4. First batch takes ~60-90 seconds (cluster startup)"
echo "5. Remaining batches are much faster"
echo "=========================================="
echo ""

/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./hdfs_streaming.py

echo ""
echo "✅ Demo completed successfully!"
echo ""
echo "🎓 What students learned:"
echo "  • How Spark Streaming processes files incrementally"
echo "  • How word counts accumulate across batches"  
echo "  • The difference between batch and streaming processing"
echo ""
echo "🔧 Useful commands:"
echo "  • Re-run demo: ./test.sh"
echo "  • Check YARN UI: http://$(hostname -I | awk '{print $1}'):8088"
