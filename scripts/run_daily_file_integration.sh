#!/bin/bash
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <date> <csvFile>"
    exit 1
fi

DATE=$1
CSV_FILE=$2
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  "$SCRIPT_DIR/../target/spark-project-1.0-SNAPSHOT.jar" \
  integration "$DATE" "$CSV_FILE"