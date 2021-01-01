# User Manual of the Project:

This is a docerized project. for running, It would be necessary to deploy all requierd services which are all ready in docker-compose file. for having them all run and ready to work, you just have to use the command below:

```bash
docker-compose up
```

By running "docker-compose up", all required services become available. They listed as below: 

1- Apache Spark (With two available workers): http://localhost:8080/

2- pgAdmin4 (A user interface for using PostgreSQL): http://localhost:5050/

3- Apache Airflow: http://localhost:8090/


Next step would be running our main python script which has a model for Time Series Forecasting. This python script will be run through another container which is created to work as the driver and call the spark cluster. The python code I created is based on the notebook that was provided by the statment of the project.

The docker container will be create directly and not by docker-compose that I proposed before. The reason that I decided to make it separet from other services is that I want to have control on the naming system of the created docker image, inorder to can push/pull it to/from a registery (In this example, I used my dockerhub account, but it can be easly change and connect to whatever registery that works better like gitlab). I have created 3 bash script inorder to make this procedure automate and just by running them, we can have the expected result. The scripts are:

docker_build_and_run.sh => Building docker image based on the docker file and run it in a proper way.

docker_push.sh => Pushing the developed docker image into the registry (In my case, I used my dockerhub account).

docker_pull_and_run.sh => Pulling the last version of the docker image that we previously push it to the registry, and run it in a proper way.


There is a python script named "test-process.py" which is implimented inorder to read the data from CSV file, load it into Spark dataframe, create the model on top of that and saving the final 90days forcasting model into the pickle file as it asked in the statment of the project. For doing that, you can simply run it localy. Just before running it, we have to install all dependencies localy, and for doing that, you might want to use hereunder command for creating local envirnoment with all dependencies installed there (It can be run in the same docker container that we created for running main python script as well).

```bash
# Creat a virtual envirnoment with all dependencies installed in python version 3.7
python3.7 -m venv env && source env/bin/activate && pip install --upgrade pip && pip install -r requirements.txt 

## Run the Script.
python test-process.py 
```
