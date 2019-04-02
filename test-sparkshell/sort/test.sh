#!/bin/bash
source ../../env.sh
spark-shell <<!EOF
val array = Array((1, 6, 3), (2, 3, 3), (1, 1, 2), (1, 3, 5), (2, 1, 2))
val rdd1 = sc.parallelize(array)
val rdd2 = rdd1.map(f => ((f._1, f._3), f))
val rdd3 = rdd2.sortByKey()
val rdd4 = rdd3.values.collect

!EOF
