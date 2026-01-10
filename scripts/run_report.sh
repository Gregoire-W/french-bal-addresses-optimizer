#!/bin/bash
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  "$SCRIPT_DIR/../target/spark-project-1.0-SNAPSHOT.jar" \
  report
