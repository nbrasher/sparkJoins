   
#!/bin/sh

JAR=`ls -1rt target/scala-2.12/sparkJoins-assembly-*.jar | tail -1`
DATA="training_data.csv"

echo Running $JAR
if [ ! -f training_data.csv ]; then
    echo Downloading latest training data
    DATA=`gsutil ls gs://eratosthenes-dev/training-data/entity_matching_training_data\* | tail -1`
    gsutil cp $DATA training_data.csv
fi
echo Using $DATA

spark-submit \
    --driver-memory 8G \
    --executor-memory 3G \
    --master 'local' \
    --class sparkJoins.Main \
    $JAR
