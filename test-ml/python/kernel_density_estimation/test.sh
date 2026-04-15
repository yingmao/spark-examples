#!/bin/bash
source ../../../env.sh
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./kernel_density_estimation.py
