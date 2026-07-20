FROM apache/airflow:2.9.1

USER root

# Docker CLI is required so BashOperator tasks can run
# "docker exec spark-master spark-submit ..."
RUN apt-get update -q && \
    apt-get install -y -q docker.io && \
    rm -rf /var/lib/apt/lists/*

USER airflow
