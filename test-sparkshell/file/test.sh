#!/bin/bash
source ../../env.sh
spark-shell <<!EOF
val textFile = sc.textFile("File:///spark-examples/test-data/test.txt")
println(textFile.count())
println(textFile.first)
val linesWithSpark = textFile.filter ( 
                        line => line.contains("Spark")
                    )
linesWithSpark.collect ()
linesWithSpark.foreach (println)

!EOF
