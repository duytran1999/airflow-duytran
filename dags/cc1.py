from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import random

import dask.dataframe as dd
import pandas as pd 




def hello_function():
    print('Hello, this is the first task of the DAG')
    time.sleep(10)


def last_function():
    print('DAG run is done.')


def sleeping_function():
    print("Sleeping for 5 seconds")
    rand_number = random.randrange(15,20)
    time.sleep(rand_number)

def handle_parquet():
    df = dd.read_csv('./dags/data_.csv')
    df = df.repartition(24)
    df["Calculated_DTR"] = df["DTR"]*int(9e15)
    result = df.groupby("CLIENTID")["Calculated_DTR"].mean().compute()
    result_parquet = pd.DataFrame(result)
    result_parquet.to_parquet("./dags/parquet_file")


def handle_large_parquet():
    temp = dd.read_parquet("./dags/train_data_parquet")
    result = temp[temp['customer_ID'] == '0000099d6bd597052cdcda90ffabf56573fe9d7c79be5fbac11a8ed792feb62a'].compute()
    print(result)

with DAG(
        dag_id="celery_executor_demo_cc",
    start_date=datetime(2021, 1, 1),
        schedule="*/1 * * * *",
        catchup=False,
        max_active_runs= 1,max_active_tasks=10,
        tags=["duytran_test"]
        ) as dag:

    task1 = PythonOperator(
        task_id="start_1",
        python_callable=hello_function
    )

    task2_1 = PythonOperator(
        task_id="sleepy_21_1",
        python_callable=sleeping_function
    )

    task2_2 = PythonOperator(
        task_id="sleepy_22_1",
        python_callable=sleeping_function
    )

    task2_3 = PythonOperator(
        task_id="sleepy_23_1",
        python_callable=sleeping_function
    )

    task3_1 = PythonOperator(
        task_id="sleepy_31_1",
        python_callable=sleeping_function
    )

    task3_2 = PythonOperator(
        task_id="sleepy_32_1",
        python_callable=handle_large_parquet
    )

    task3_3 = PythonOperator(
        task_id="sleepy_33_1",
        python_callable=sleeping_function
    )

    task3_4 = PythonOperator(
        task_id="sleepy_34_1",
        python_callable=sleeping_function
    )

    task3_5 = PythonOperator(
        task_id="sleepy_35_1",
        python_callable=handle_large_parquet
    )

    task3_6 = PythonOperator(
        task_id="sleepy_36_1",
        python_callable=sleeping_function
    )

    task4_1 = PythonOperator(
        task_id="sleepy_41_1",
        python_callable=handle_large_parquet
    )

    task4_2 = PythonOperator(
        task_id="sleepy_42_1",
        python_callable=handle_large_parquet
    )

    task5 = PythonOperator(
        task_id="sleepy_5_1",
        python_callable=sleeping_function
    )

    task6 = PythonOperator(
        task_id="end_1",
        python_callable=last_function
    )

task1 >> [task2_1, task2_2, task2_3]
task2_1 >> [task3_1, task3_2]
task2_2 >> [task3_3, task3_4]
task2_3 >> [task3_5, task3_6]
[task3_1, task3_2, task3_3] >> task4_1
[task3_4, task3_5, task3_6] >> task4_2
[task4_1, task4_2] >> task5 >> task6