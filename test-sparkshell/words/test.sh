#!/bin/bash
source ../../env.sh
spark-shell <<!EOF
val textFile = sc.textFile("File:///spark-examples/test-data/test.txt")
val topWordCount = textFile.flatMap(str=>str.split(" ").filter(!_.isEmpty).map(word=>(word,1))).reduceByKey(_+_).map{case (word, count) => (count, word)}.sortByKey(false)
topWordCount.take(5).foreach(x=>println(x))
!EOF
