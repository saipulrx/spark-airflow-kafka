# Hands On Apache Spark, Airflow & Kafka

This repository for online mini bootcamp Apache Spark for Data Engineer in Ruang Data Project. In this course use tech stack :
- Data Ingestion & Transformation (Batch) : Apache Spark

- Data Ingestion & Transformation (Stream) : Apache Kafka & Spark Streaming

- Workflow Orchestration : Apache Airflow
- Data Platform : OLTP(PostgreSQL)

Prerequisite : 
- Already Install Docker Desktop for Windows OS or any docker software for Linux/Mac Os
- Have knowledge Basic Python & SQL
- Have knowledge Basic Docker & Docker Compose

Step by step to run Apache Spark, Airflow & Kafka with use Docker
- git clone this repo
- run `make docker-build or make docker-build-arm` for first time only to build docker images. docker-build for machine amd64 and docker-build-arm for machine arm64.
- run `make postgres` to run postgres container
- run `make spark` to run spark cluster container
- run `make jupyter` to spin up jupyter notebook for testing and validation purposes
- run `make airflow` to Spinup airflow scheduler and webserver
- run `make kafka` to Spinup kafka cluster (Kafka+Zookeeper)
- run `make clean` to Cleanup all running containers related to the challenge

NB : this repo is from source repo https://github.com/thosangs/dibimbing_spark_airflow with some modification and additional code