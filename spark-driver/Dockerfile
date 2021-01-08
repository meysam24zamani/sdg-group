FROM python:3.7

##Uncomment line below, if you want to use the last image version saved in registry instead of based image of puckel/docker-airflow.
#FROM docker.io/meysam24zamani/spark-driver:v1.0.0

WORKDIR /usr/src/app

RUN pip install --upgrade pip

RUN export PYSPARK_SUBMIT_ARGS="--master local[2]"

RUN apt-get update
RUN apt-get install default-jdk -y

COPY scripts/requirements.txt ./

RUN pip install -r requirements.txt

RUN mkdir dataset

ADD scripts/dataset /usr/src/app/dataset

COPY scripts/process-individual-stores-items.py scripts/process-single-forecast.py spark-3.0.1-bin-hadoop2.7.tgz ./

RUN tar -xzf spark-3.0.1-bin-hadoop2.7.tgz

RUN rm  spark-3.0.1-bin-hadoop2.7.tgz

#CMD ./spark-3.0.1-bin-hadoop2.7/bin/spark-submit --num-executors 2 --executor-cores 2 --executor-memory 1g --driver-cores 1 --driver-memory 1g process-individual-stores-items.py
