#!/bin/bash
source ../../env.sh
spark-shell <<!EOF
val pairs = sc.parallelize( List( 
                ("This", 2), 
                ("is", 3), 
                ("Spark", 5), 
                ("is", 3) 
                                        ) )
pairs.collect().foreach(println)
val pair1 = pairs.reduceByKey((x,y) => x+y, 4)
pair1.collect.foreach(println)
!EOF
