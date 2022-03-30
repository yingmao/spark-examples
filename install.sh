#!/bin/sh
python3 /spark-examples/setup.py
cat workers | while read line
do
    if [ "$line" = "-" ]; then
        echo "Skip $line"
    else
        ssh root@$line -n "rm -rf /spark-examples/ && mkdir /spark-examples/"
        echo "Copy data to $line"
        scp /spark-examples/hadoop-3.3.1.tar.gz root@$line:/spark-examples/ &&  scp /spark-examples/spark-3.2.1-bin-hadoop3.2.tgz root@$line:/spark-examples/ && scp /spark-examples/setup.py root@$line:/spark-examples/ && scp /spark-examples/manager root@$line:/spark-examples/ && scp /spark-examples/workers root@$line:/spark-examples/
        echo "Setup $line"
        ssh root@$line -n "cd /spark-examples/ && python3 setup.py"
        echo "Finished config node $line"
    fi
done

manager=$(cat /spark-examples/manager)
echo "export SPARK_MASTER=$manager" > env.sh
