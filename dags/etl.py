from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


qtr_check_lasted_table = """
  SELECT MAX(XXX._TABLE_SUFFIX) as lasted_dataset
    FROM ( SELECT DISTINCT _TABLE_SUFFIX FROM `production-vdsc.analytics_327961316.*`) AS XXX
"""

path_credential_file =  '/opt/airflow/dags/production-vdsc.json'#"production-vdsc.json" #
path_metadata =  '/opt/airflow/dags/smart_dragon_metadata.json'#"smart_dragon_metadata.json" #


@dag(
    dag_id="lasted_dataset",
    schedule="*/1 * * * *",
    start_date=datetime(2021, 1, 1),
    max_active_runs= 1,max_active_tasks=1,
    catchup=False,
    tags=["smartdragon", "bigquery", "python", "gcp"],
)
def task_flow():
    @task(task_id='get_lasted_dataset_smartdragon')
    def get_lasted_dataset_smartdragon():
        import pandas as pd
        from google.oauth2 import service_account

        qtr_check_lasted_table = """
        select distinct _TABLE_SUFFIX as list_dataset
        from `production-vdsc.analytics_327961316.*`
        order by _TABLE_SUFFIX desc
        limit 1
        """

        credentials = service_account.Credentials.from_service_account_file(
            path_credential_file,
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
        )
        df = pd.read_gbq(qtr_check_lasted_table, credentials=credentials)
        lasted_dataset_in_gbq = df['list_dataset'][0]
        context = get_current_context()
        ti = context["ti"]
        ti.xcom_push("lasted_dataset", lasted_dataset_in_gbq)
        print(lasted_dataset_in_gbq)
        return lasted_dataset_in_gbq

    @task.branch(task_id='check_exist_dataset')
    def check_exist_dataset():
        import pandas as pd
        db = pd.read_json(path_metadata)
        db = db.sort_values(by=['list_dataset'], ascending=False)

        context = get_current_context()
        ti = context["ti"]
        lasted_dataset = ti.xcom_pull(task_ids=["get_lasted_dataset_smartdragon"], key='lasted_dataset')

        if (db.isin([lasted_dataset[0]]).any().any() == True):
            return "khong_lam_gi_ca"
        else:
            return "update_lasted_dataset_in_db"

    @task(task_id="update_lasted_dataset_in_db", retries=2)
    def update_lasted_dataset():  # def update_lasted_dataset(new_value):
        context = get_current_context()
        ti = context["ti"]
        lasted_dataset = ti.xcom_pull(task_ids=["get_lasted_dataset_smartdragon"], key='lasted_dataset')
        print(lasted_dataset)
        import pandas as pd
        db = pd.read_json(path_metadata)
        db = db.sort_values(by=['list_dataset'], ascending=False)
        db = db.append([{'list_dataset': lasted_dataset[0]}], ignore_index=False)
        db = db.sort_values(by=['list_dataset'], ascending=False).reset_index(drop=True)
        db.to_json(path_metadata)
        print("co_lam")

    @task(task_id="khong_lam_gi_ca", retries=2)
    def khong_lam_gi_ca():
        print("khong_lam_gi_ca")

    


    # FLOW
    cc1 = khong_lam_gi_ca()
    cc2 = update_lasted_dataset()
    get_lasted_dataset_smartdragon() >> check_exist_dataset() >> [cc1, cc2]



main_flow = task_flow()
