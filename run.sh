#!/bin/bash

declare -a args
for (( i=0; i<=44; i++ ))
do
   #echo $i
   text=/user/data/$i
   args+=($text)
done

echo ${args[@]}

spark-submit \
   --master spark://ip-10-0-0-13:7077 \
   --executor-memory 6G \
   --driver-memory 6G \
   --num-executors 4 \
   --executor-cores 1 \
   --conf spark.executor.memoryOverhead=1024 \
   --conf "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
   --conf "spark.driver.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
   --total-executor-cores 4 \
   inverted-indexing-dev.py ${args[@]}

hadoop fs -getmerge /user/ubuntu/output/reverse_index.txt ./output.csv
