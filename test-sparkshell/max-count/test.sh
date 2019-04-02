#!/bin/bash
source ../../env.sh
spark-shell <<!EOF
val textFile = sc.textFile("File:///spark-examples/test-data/test.txt")
val res = textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
println(res)

!EOF
