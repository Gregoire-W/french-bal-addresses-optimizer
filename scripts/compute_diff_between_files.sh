#!/bin/bash

# 1. Vérification du nombre d'arguments
if [ "$#" -ne 2 ]; then
    echo "Erreur: Mauvais nombre d'arguments."
    echo "Usage: $0 <parquetDir1> <parquetDir2>"
    exit 1
fi

# 2. Assignation des variables
PATH1=$1
PATH2=$2

# 3. Récupération du chemin du script pour localiser le JAR
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")

# 4. Lancement de Spark
spark-submit \
  --class fr.esilv.SparkMain \
  --master local[*] \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:$SCRIPT_DIR/../src/main/resources/log4j.properties" \
  "$SCRIPT_DIR/../target/spark-project-1.0-SNAPSHOT.jar" \
  diff "$PATH1" "$PATH2"