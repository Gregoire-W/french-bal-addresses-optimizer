# cmd: docker exec -it spark_project_env bash

# On utilise Eclipse Temurin qui est le successeur officiel et fiable
FROM eclipse-temurin:11-jdk-focal

# Installation de Maven et des outils de base
RUN apt-get update && apt-get install -y maven wget procps

# Installation de Spark 3.5.0
ENV SPARK_VERSION=3.5.0
RUN wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop3 /opt/spark \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Configuration des chemins
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /app