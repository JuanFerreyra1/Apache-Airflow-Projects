from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

with DAG(
	"my-first-dag",
	description = "This is my first attempt to create my own DAG",
	tags=["own dag"],
	schedule=None,
	start_date =datetime(2023, 1, 1),
) as dag:
    tres_task = BashOperator(
    	task_id="Listhometres",
    	bash_command="echo {AIRFLOW_HOME}",
    	)
    def read_file():
    	AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    	file = open(AIRFLOW_HOME+"/employees.csv","r")
    	print(file.read())    	
    	return "ok"
    second_task = PythonOperator(
    	task_id='a11FileFromLocalDirectory',
    	python_callable=read_file,
    )
