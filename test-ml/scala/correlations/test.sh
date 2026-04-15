#!/bin/bash
source ../../../env.sh
mvn clean package
/usr/local/spark/bin/spark-submit --class CorrelationsExample --master=spark://$SPARK_MASTER:7077 target/Correlations-1.0-SNAPSHOT.jar
mvn clean
