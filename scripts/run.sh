#!/bin/sh

JAR=`ls -1rt target/scala-2.12/sparkJoins-assembly-*.jar | tail -1`
DATA="training_data.csv"

echo Running $JAR
echo Using $DATA

spark-submit \
    --driver-memory 8G \
    --executor-memory 3G \
    --master 'local' \
    --class sparkJoins.Main \
    $JAR
