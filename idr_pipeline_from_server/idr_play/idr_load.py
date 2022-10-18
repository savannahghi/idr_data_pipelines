import requests, sys
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow import models
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


sys.path.append('/home/airflow/gcs/dags/idr_play/dependencies')
import parquet_solution

GOOGLE_CONN_ID = "google_cloud_default"
LOCATION = models.Variable.get("location")
PROJECT_ID= models.Variable.get("PROJECT_ID")

COVID_STAGING_DATASET = models.Variable.get("IDR_COVID_play")
MMD_STAGING_DATASET = models.Variable.get("MMD_play")
HTS_STAGING_DATASET = models.Variable.get("HTS_play")
VLS_STAGING_DATASET = models.Variable.get("VLS_play")

mmd_table_id = f"{PROJECT_ID}.{MMD_STAGING_DATASET}.staging"
covid_table_id = f"{PROJECT_ID}.{COVID_STAGING_DATASET}.staging"
hts_table_id = f"{PROJECT_ID}.{HTS_STAGING_DATASET}.staging"
vls_table_id = f"{PROJECT_ID}.{VLS_STAGING_DATASET}.staging"

def alert():
    import mattermost
    a = mattermost.mattermost_alert()
    return a 

def load_mmd():
    folder = models.Variable.get("mmd_folder")
    a = parquet_solution.load_data(mmd_table_id,folder)
    return a

def load_covid():
    folder = models.Variable.get("covid_folder")
    a = parquet_solution.load_data(covid_table_id,folder)
    return a

def load_hts():
    folder = models.Variable.get("hts_folder")
    a = parquet_solution.load_data(hts_table_id,folder)
    return a

def load_vls():
    folder = models.Variable.get("vls_folder")
    a = parquet_solution.load_data(vls_table_id,folder)
    return a

def publish_messages():
    import publisher
    topic = models.Variable.get("IDR_publish_messages_play")
    a = publisher.publish_messages(PROJECT_ID, topic)
    return a

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': alert
}

'''Define the DAG'''

with DAG("idr_load_stage__play", start_date=dt(2022, 9, 9),
        schedule_interval='0 3 * * *',
        default_args=default_args) as dag:

    load_dataset_MMD = PythonOperator(
        task_id='load_MMD',
        python_callable=load_mmd,
        dag=dag
        )

    load_dataset_VLS = PythonOperator(
        task_id='load_VLS',
        python_callable=load_vls,
        dag=dag
        )
    
    load_dataset_HTS = PythonOperator(
        task_id='load_HTS',
        python_callable=load_hts,
        dag=dag
        )

    load_dataset_COVID = PythonOperator(
        task_id='load_COVID',
        python_callable=load_covid,
        dag=dag
        )

    publish_events = PythonOperator(
        task_id='publish_messages',
        python_callable=publish_messages,
        dag=dag
        )
    
    trigger = TriggerDagRunOperator(
        task_id ='trigger',
        trigger_dag_id='keemr_mmd_transforms__play',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

'''Define the task dependencies'''

load_dataset_MMD >> load_dataset_VLS >> load_dataset_HTS >> load_dataset_COVID >> publish_events
publish_events >> trigger
