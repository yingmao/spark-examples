#!/bin/sh
python3 /spark-install/setup.py
cat workers | while read line
do
    if [ "$line" = "-" ]; then
        echo "Skip $line"
    else
        ssh root@$line -n "rm -rf /spark-install/ && mkdir /spark-install/"
        echo "Copy data to $line"
        scp  /spark-install/setup.py root@$line:/spark-install/ && scp /spark-install/manager root@$line:/spark-install/ && scp /spark-install/workers root@$line:/spark-install/
        echo "Setup $line"
        ssh root@$line -n "cd /spark-install/ && python3 setup.py"
        echo "Finished config node $line"
    fi
done

manager=$(cat /spark-install/manager)
echo "export SPARK_MASTER=$manager" > env.sh
