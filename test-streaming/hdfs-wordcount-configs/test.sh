#!/bin/bash
set -euo pipefail

# ============================================================
# Spark Streaming Parameter Experiments
# Usage: ./test.sh [variation] 
# 
# Variations:
#   1 = Batch sizing (maxFilesPerTrigger)
#   2 = Output modes (complete vs update)  
#   3 = Tumbling windows (time-based grouping)
#   4 = Threshold filtering (significance detection)
#
# Examples:
#   ./test.sh 1    → Batch sizing experiment
#   ./test.sh 3    → Windowing experiment
# ============================================================

VARIATION=${1:-1}

# Variation descriptions for student reference
declare -A DESCRIPTIONS=(
    [1]="Batch Sizing: How maxFilesPerTrigger affects processing granularity"
    [2]="Output Modes: How complete vs update changes result emission"
    [3]="Tumbling Windows: How time-based grouping works"
    [4]="Threshold Filtering: How to focus on significant events"
)

declare -A SCRIPTS=(
    [1]="hdfs_streaming_v1_batch.py"
    [2]="hdfs_streaming_v2_output.py" 
    [3]="hdfs_streaming_v3_window.py"
    [4]="hdfs_streaming_v4_filter.py"
)

declare -A CONCEPTS=(
    [1]="Throughput vs Latency trade-offs"
    [2]="State management and result emission strategies"
    [3]="Event-time processing and temporal aggregation"
    [4]="Real-time filtering and significance detection"
)

if [[ ! ${DESCRIPTIONS[$VARIATION]+_} ]]; then
    echo "❌ Invalid variation: $VARIATION"
    echo "   Valid options: 1, 2, 3, 4"
    exit 1
fi

echo "========================================================"
echo "  SPARK STREAMING PARAMETER EXPERIMENT #$VARIATION"
echo "========================================================"
echo "📖 Focus: ${DESCRIPTIONS[$VARIATION]}"
echo "🎓 Concept: ${CONCEPTS[$VARIATION]}"
echo "========================================================"

# Standard setup
echo "🧹 Cleaning previous applications..."
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    /usr/local/hadoop/bin/yarn application -kill "$app" >/dev/null 2>&1 || true
done

echo "📁 Preparing HDFS directories..."
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /streaming-test/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /streaming-test/input

echo "📝 Creating enhanced sample data..."
python3 ./generate_data.py

echo ""
echo "🚀 Starting Parameter Experiment #$VARIATION"
echo "========================================================"

/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --conf spark.sql.adaptive.enabled=false \
    ./"${SCRIPTS[$VARIATION]}"

echo ""
echo "========================================================"
echo "✅ Experiment #$VARIATION completed!"
echo ""
echo "🎓 Key Takeaways:"
case "$VARIATION" in
    1)
        echo "  • maxFilesPerTrigger controls batch granularity"
        echo "  • Larger batches = better throughput, higher latency"
        echo "  • Smaller batches = lower latency, more overhead"
        ;;
    2)
        echo "  • 'complete' mode shows all accumulated results"
        echo "  • 'update' mode shows only changed results"
        echo "  • Choice depends on downstream system requirements"
        ;;
    3)
        echo "  • Tumbling windows create independent time buckets"
        echo "  • Useful for 'last X seconds' analysis"
        echo "  • Different from global accumulation"
        ;;
    4)
        echo "  • Threshold filtering reduces noise in real-time systems"
        echo "  • Essential for alerting and trend detection"
        echo "  • Higher thresholds = more selective results"
        ;;
esac
echo ""
echo "🔧 Try other experiments:"
echo "  ./test.sh 1  → Batch sizing effects"
echo "  ./test.sh 2  → Output mode differences"  
echo "  ./test.sh 3  → Time-based windowing"
echo "  ./test.sh 4  → Significance filtering"
echo "========================================================"
