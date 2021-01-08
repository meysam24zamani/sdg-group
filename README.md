# User Manual of the Project:

This is a docerized project. for running, It would be necessary to deploy requierd services. For having them all run and ready to work, you just need to use the command below:

```bash
docker-compose up
```

By running "docker-compose up", all required services become available. They listed as below: 

1- Apache Spark (With two available workers): http://localhost:8080/

2- pgAdmin4 (A user interface for using PostgreSQL): http://localhost:5050/

3- Apache Airflow: http://localhost:8090/


Next step would be running our main python scripts which has a model for Time Series Forecasting. These python scripts will be run through another container which is created to work as a driver for calling the spark cluster (Also known as spark-submit). The python codes are based on the notebook that was provided by the statment of the project and developed by me.

The spark-driver docker container will be created directly and not by docker-compose that I proposed before. The reason that I decided to make it separet from other services is that I wanted to have control on the naming system of the created docker image, inorder to can push/pull it to/from a registery (In this example, I have conected it to my dockerhub account, but it can be easly change and connect to whatever registery that works better like gitlab). I have created 4 bash script inorder to make this procedure automate and just by running them, we can have the expected result. For using them you have to navigate into the "spark-driver" folder. The list of the bash scripts and their descriptions are available as below:

docker_build_and_run.sh => Building docker image based on the docker file and run it in a proper way.

docker_run.sh => Running different scripts based on the docker image that we built it previously.

docker_push.sh => Pushing the developed docker image into the registry (In my case, I used my dockerhub account).

docker_pull_and_run.sh => Pulling the last version of the docker image that we previously push it to the registry, and run it in a proper way.


The python scripts that are run through these bash scripts, are placed in "scripts" directory. The list of the python scripts and their descriptions are available as below: 

SCRIPT1 => process-individual-stores-items.py : Training time series forecasting models in parallel with Spark through the pandas user-defined functions (UDFs). This file would be the one that we are intrested to submit it into the Spark and for doing that we will use the spark-driver docker container.

SCRIPT2 => process-single-forecast.py : Read the data from CSV file, load it into the Spark dataframe, create the model on top of that and saving the final 90days forcasting model into the pickle file (Located in "trained-model" directory as it asked in the statment of the project). This script can be run either through the spark-driver docker container or localy by local environment.


SCRIPT3 => process-single-forecast-postgresql.py : The same as SCRIPT2 but this one is loading the final result in PostgreSQL. For doing that, its necessary to create the target schema in PostgreSQL. SCRIPT4 which is "connection.py", would make the connection to the PostgreSQl, and SCRIPT5 which is "schema.py", would create all tables in to the PostgreSQL database. In the root of the repository, you can find a diagram of the created target schema to understand better the relations between tables.


SCRIPT4 => connection.py : Make connection to the PostgreSQL. We are using it as a module and import it to the SCRIPT3 and SCRIPT5.


SCRIPT5 => schema.py : Create all tables in to the PostgreSQL database. This script has to run just once and that would be in the beginning for creating all tables in PostgreSQL.


For running SCRIPT3 and SCRIPT5, you can simply run them localy. Just before running them, it would be necessary to install all dependencies localy, and for doing that, you might want to use hereunder command for creating local envirnoment with all dependencies installed there.

```bash
# Creat a virtual envirnoment with all dependencies installed in python version 3.7 
python3.7 -m venv env && source env/bin/activate && pip install --upgrade pip && pip install -r requirements.txt 

## Run the Script.
python $SCRIPT3 or $SCRIPT5 
```

## Airflow:

Apache Airflow is a workflow automation and scheduling system that can be used to author and manage data pipelines. Airflow uses workflows made of directed acyclic graphs (DAGs) of tasks. In this project, I have created a dag named "run_pyspark_app.py" which it supposed to run all the scripts from 3 paths. One path for running "process-individual-stores-items.py" by using SparkSubmitOperator. The other path would be running "schema.py" and "process-single-forecast-postgresql.py" inorder to have the result of single item/store forecast in PostgreSQL, and the last path for running "process-single-forecast.py" which will show the results the same as previous script in to the logs page.
