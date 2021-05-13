import logging

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.bash_operator import BashOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_connector_test", default_args=args, schedule_interval='@once'
)



def row_count(**context):
    sleep(120)
    dwh_hook = SnowflakeHook(snowflake_conn_id="ocudm")
    result = dwh_hook.get_first("select count(*) from curo_medclaims")
    sleep(120)
    logging.info("Number of rows in `curo_medclaims`  - %s", result[0])


with dag:
    get_count = PythonOperator(task_id="get_count", python_callable=row_count)
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 30')
    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')

print_hello >> sleep >> get_count
