from datetime import timedelta
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('docker_operator_test_cc_11232',
          default_args=default_args,
          schedule="*/1 * * * *",
          start_date=days_ago(2)
          )
dop = DockerOperator(
    task_id='docker_op_test',
    image='stock_image:v1.0.0',
    command='python3 stock.py',
    docker_url='TCP://docker-socket-proxy:2375',
    network_mode='bridge',
    dag=dag, 
    xcom_all = True,
    retrieve_output= True,
    retrieve_output_path= '/tmp/script.out',
    mem_limit = '512m',
    auto_remove = True
)
dop