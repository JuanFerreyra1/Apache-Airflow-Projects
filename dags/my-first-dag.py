from datetime import datetime
import os 
import platform
import boto3
import re
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator


from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable 

from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor

#provider packages
#spark
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#aws
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
#postgres
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.models.xcom import XCom


#environment
airflow_home = os.getenv('HOME')



dag = DAG(dag_id="daily_insert",schedule=None,start_date =datetime(2023, 1, 1))



#first_task

read_task = SparkSubmitOperator(
    application="{}/airflow/spark_jobs/first-task-spark.py".format(airflow_home),
    task_id = "spark_read_file",
    conn_id = "spark_c",
    jars="{}/airflow/spark_jobs/jars/postgresql-42.6.0.jar".format(airflow_home),
    driver_class_path="{}/airflow/spark_jobs/jars/postgresql-42.6.0.jar".format(airflow_home),
    dag = dag
)

#second_task
query_task = PostgresOperator(
    task_id="query_data_test",
    sql="""
    select x.country,max(x.count_of_countries) from (
        select country,count(1) as count_of_countries from project1.public.pets
        group by country)x
    group by x.country
    limit 1;""",
    postgres_conn_id="postgres_default",
    dag = dag
    )


#third_task
def function_sns(ti):

    def connection_keys_aws():
        with open('{}/airflow/credentials/keys'.format(airflow_home)) as f:
            lines = f.read()
            access_key = re.search("(?<=aws_access_key_id=).*",lines)[0]
            secret_access_key = re.search("(?<=aws_secret_access_key=).*",lines)[0]
            credentials = [access_key,secret_access_key]
        return credentials
    

    credentials = connection_keys_aws()
    value = ti.xcom_pull(task_ids='query_data_test',key='return_value')

    def api_call(credentials):
        sns_client = boto3.client(
            'sns',
            aws_access_key_id = credentials[0],
            aws_secret_access_key = credentials[1])
        
        sns_client.publish(
            TopicArn = 'arn:aws:sns:us-east-1:440185723682:TopicFromAirflow',
            Message = str(tuple(value[0])))
    
    api_call(credentials)
    return "ok"

message_sns_task = PythonOperator(
    task_id = "send_message_sns",
    python_callable=function_sns
    )




read_task>>query_task>>message_sns_task
