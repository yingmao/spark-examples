#!/bin/bash
set -euo pipefail

echo "=== Spark DataFrame Examples Test Suite ==="

# Kill any running applications
echo "=== Cleaning previous applications ==="
for app in $(/usr/local/hadoop/bin/yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep application_ | awk '{print $1}' || true); do
    echo "Killing application: $app"
    /usr/local/hadoop/bin/yarn application -kill "$app"
done

echo "=== Generating test data ==="
mkdir -p ./test-data

# Example 1 data - parking violations
cat > ./test-data/violations.csv << 'EOF'
plate_id,vehicle_color,state,violation_code,street_code
ABC123,BLK,NY,21,10001
XYZ789,WHITE,ny,14,10002
DEF456,Blk,NEW YORK,20,10001
GHI789,wh,NY,38,10003
JKL012,RED,NJ,21,10002
MNO345,BLACK,NY,14,10001
EOF

# Example 2 data - more violations for revenue analysis
cat > ./test-data/violations_revenue.csv << 'EOF'
violation_code,street_code,plate_id
21,SC001,ABC123
14,SC002,XYZ789
21,SC001,DEF456
20,SC003,GHI789
38,SC001,JKL012
14,SC002,MNO345
21,SC001,PQR678
20,SC003,STU901
EOF

# Example 3 data - user events for sessionization
cat > ./test-data/user_events.csv << 'EOF'
user_id,timestamp,event_type,price
1,1640995200,view,0
1,1640995500,addtocart,0
1,1640995800,purchase,50
1,1640997600,view,0
2,1640995000,view,0
2,1640997400,purchase,75
2,1640999200,view,0
3,1640994800,view,0
3,1640995100,purchase,30
EOF

# Example 4 data - customer RFM data
cat > ./test-data/customer_rfm.csv << 'EOF'
customer_id,recency,frequency,monetary
C001,10,15,1200
C002,45,8,800
C003,5,25,2500
C004,90,3,150
C005,30,12,950
C006,75,2,100
C007,15,20,1800
C008,60,5,400
EOF

echo "=== Preparing HDFS ==="
/usr/local/hadoop/bin/hdfs dfs -rm -r -f /spark_examples/ || true
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /spark_examples/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./test-data/*.csv /spark_examples/input/

echo "=== Running Example 1: Basic DataFrame Operations ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./example1_basic_dataframe.py \
    hdfs:///spark_examples/input/violations.csv \
    hdfs:///spark_examples/output1/

echo "=== Running Example 2: Broadcast Join Revenue Analysis ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./example2_broadcast_join.py \
    hdfs:///spark_examples/input/violations_revenue.csv \
    hdfs:///spark_examples/output2/

echo "=== Running Example 3: Window Functions Sessionization ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    ./example3_window_functions.py \
    hdfs:///spark_examples/input/user_events.csv \
    hdfs:///spark_examples/output3/

echo "=== Running Example 4: MLlib K-Means Pipeline ==="
/usr/local/spark/bin/spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    ./example4_kmeans_pipeline.py \
    hdfs:///spark_examples/input/customer_rfm.csv \
    hdfs:///spark_examples/output4/

echo "=== Viewing sample results ==="
echo "Example 1 - Color distribution:"
/usr/local/hadoop/bin/hdfs dfs -cat /spark_examples/output1/*.csv | head -5

echo -e "\nExample 4 - Customer segments:"
/usr/local/hadoop/bin/hdfs dfs -cat /spark_examples/output4/*.csv | head -10

echo "=== All Spark DataFrame examples completed successfully! ==="
echo "You're now ready to tackle Lab 3 with confidence!"