#!/bin/bash
source ../../env.sh
mvn clean package
/usr/local/spark/bin/spark-submit --class SparkPi --master=spark://$SPARK_MASTER:7077 target/PI-1.0-SNAPSHOT.jar
mvn clean
