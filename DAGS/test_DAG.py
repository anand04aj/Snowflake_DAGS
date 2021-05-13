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

create_insert_query = [
    """create table public.test_table (amount number);""",
    """insert into public.test_table values(1),(2),(3);"""
]

#def row_count(**kwargs):
#    sleep(120)
#    dwh_hook = SnowflakeHook(snowflake_conn_id="ocudm")
#    result = dwh_hook.get_first("select count(*) from curo_medclaims")
#    print("hello")
#    sleep(120)
#    logging.info("Number of rows in `curo_medclaims`  - %s", result[0])


with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=create_insert_query,
        snowflake_conn_id="ocudm",
    )
    get_count = PythonOperator(task_id="get_count", python_callable=row_count,provide_context=True)
    sleep = BashOperator(task_id='sleep',
                         bash_command='sleep 30')
    print_hello = BashOperator(task_id='print_hello',
                               bash_command='echo "hello"')

print_hello >> sleep >> create_insert
