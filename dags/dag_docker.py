from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime


@dag(
    dag_id="docker_operator_test",
    schedule="*/1 * * * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs= 1,max_active_tasks=1,
    catchup=False,
    tags=["docker", "test", "python", "duytran"],
)
def docker_dag():
    @task(task_id="start")
    def t1():
        pass

    t2 = DockerOperator(
        task_id='t2',
        api_version ="auto",
        docker_url ='tcp://docker-socket-proxy:2375',
        auto_remove=False,
        image='docker-airflow-etl:latest',
        command='src/task.py',
        force_pull=False
    )
    t1() >> t2


dag_ = docker_dag()
