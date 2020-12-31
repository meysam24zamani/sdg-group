FROM python:3.7

# Uncomment line below, if you want to use the last image version saved in registry instead of based image of puckel/docker-airflow.
# FROM docker.io/meysam24zamani/python-Meysam:v1.0.0

WORKDIR /usr/src/app

RUN pip install --upgrade pip

RUN export SPARK_HOME="$HOME/spark-1.5.1"
RUN export PYSPARK_SUBMIT_ARGS="--master local[2]"

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY connection.py process.py ./



