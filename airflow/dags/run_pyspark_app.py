from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

def print_message():
    print("This is the technical interview of SDG_Group company!!!")

default_args = {
    "owner": "sdgGroup",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["meysam24zamani@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}





dag = DAG("pyspark_job", 
    default_args=default_args, 
    # schedule_interval=timedelta(1)
    schedule_interval='* * * * *' #=> If we want to schadule it for every-minute
    )

task_start = DummyOperator(
    task_id='Start',
    dag=dag,
)

task_welcome_python_operator = PythonOperator(task_id='print_message_welcome',
                                    python_callable=print_message, dag=dag)



#spark_master = "spark://spark:7077"
spark_master = Variable.get("spark_master")
spark_config = {
    'conn_id': 'spark_local',
    'application': '/usr/local/airflow/spark/process-individual-stores-items.py',
    'driver_memory': '1g',
    'executor_cores': 1,
    'num_executors': 1,
    'executor_memory': '1g'
}
#spark_config = Variable.get("spark_config")

task_spark_operator = SparkSubmitOperator(task_id='spark_submit_task', dag=dag, conf={"spark.master":spark_master}, **spark_config)


task_build_env = BashOperator(
        task_id='build_env'
        ,bash_command="python3.7 -m venv env && source env/bin/activate && pip install --upgrade pip && pip install -r spark/requirements.txt"
        ,dag=dag
    )

task_schema_creator = BashOperator(
        task_id='Create_Schema_PostgreSQL'
        ,bash_command="python spark/schema.py"
        ,dag=dag
    )


task_insert_data = BashOperator(
        task_id='insert_data_PostgreSQL_single_forecast'
        ,bash_command="python spark/process-single-forecast-postgresql.py"
        ,dag=dag
    )


task_single_forecast = BashOperator(
        task_id='task_single_forecast'
        ,bash_command="python spark/process-process-single-forecast.py"
        ,dag=dag
    )


task_end = DummyOperator(
    task_id='End',
    dag=dag,
)


task_start >> task_welcome_python_operator
task_welcome_python_operator >> task_spark_operator
task_spark_operator >> task_end


task_start >> task_welcome_python_operator
task_welcome_python_operator >> task_build_env
task_build_env >> task_single_forecast
task_single_forecast >> task_end


task_start >> task_welcome_python_operator
task_welcome_python_operator >> task_build_env
task_build_env >> task_schema_creator
task_schema_creator >> task_insert_data
task_insert_data >> task_end

if __name__ == "__main__":
    dag.cli()