from airflow import DAG
from datetime import datetime, timedelta
 
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
 
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
 
dag = DAG(
    'sr_etl2',
    default_args=default_args,
    schedule_interval='@once'
)
 
 
snowflake = SnowflakeOperator(
    task_id='snowflake_task',
    sql="select * from CURO_CLAIMS.curo_medclaims LIMIT 5",
    snowflake_conn_id='ocudm',
    dag=dag
)
