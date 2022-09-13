import requests
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")

COVID_BUCKET_NAME = models.Variable.get("COVID_test_bucket")
MMD_BUCKET_NAME = models.Variable.get("MMD_test_bucket")
HTS_BUCKET_NAME = models.Variable.get("HTS_test_bucket")
VLS_BUCKET_NAME = models.Variable.get("VLS_test_bucket")

COVID_STAGING_DATASET = models.Variable.get("IDR_COVID_test")
MMD_STAGING_DATASET = models.Variable.get("MMD_test")
HTS_STAGING_DATASET = models.Variable.get("HTS_test")
VLS_STAGING_DATASET = models.Variable.get("VLS_test")

LOCATION = models.Variable.get("LOCATION")

webhook = models.Variable.get("mattermost_webhook")
table_id = f"{PROJECT_ID}.{MMD_STAGING_DATASET}.staging"


def mattermost_alert(context, notifications_webhook=webhook):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    logs_url = context.get("task_instance").log_url
    headers = {}
    message = '{"text": "The DAG: '+str(dag_id)+' has failed on task: '+str(task_id)+' Logs can be viewed on: '+str(logs_url)+'"}'
    response = requests.post(notifications_webhook, headers=headers, data=message)
    return response

def load_data():
    import parquet_solution

    a = parquet_solution.load_table_dataframe(table_id)
    return a

def publish_messages():
    import publisher

    topic = models.Variable.get("IDR_publish_messages_test")
    a = publisher.publish_messages(PROJECT_ID, topic)
    return a

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': mattermost_alert
}

def my_function():
    import sys
    
    sys.path.append('/home/airflow/gcs/dags/idr/dependencies')

    import parquet_solution

    a = parquet_solution.load_table_dataframe(table_id)

    return a

'''Define the DAG'''

with DAG("idr_load_stage_test", start_date=dt(2022, 9, 9),
        schedule_interval='0 4 * * *',
        default_args=default_args) as dag:

    load_dataset_MMD = PythonOperator(
        task_id='load_MMD',
        python_callable=load_data,
        dag=dag
        )

    load_dataset_VLS = GCSToBigQueryOperator(
        task_id = 'load_dataset_VLS',
        bucket = VLS_BUCKET_NAME,
        source_objects = ['*'],
        destination_project_dataset_table = f'{PROJECT_ID}:{VLS_STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )
    
    load_dataset_HTS = GCSToBigQueryOperator(
        task_id = 'load_dataset_HTS',
        bucket = HTS_BUCKET_NAME,
        source_objects = ['*'],
        destination_project_dataset_table = f'{PROJECT_ID}:{HTS_STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    load_dataset_COVID = GCSToBigQueryOperator(
        task_id = 'load_dataset_COVID',
        bucket = COVID_BUCKET_NAME,
        source_objects = ['*'],
        destination_project_dataset_table = f'{PROJECT_ID}:{COVID_STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        )

    publish_events = PythonOperator(
        task_id='publish_messages',
        python_callable=publish_messages,
        dag=dag
        )

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        )

'''Define the task dependencies'''

load_dataset_MMD >> [load_dataset_VLS, load_dataset_HTS, load_dataset_COVID]  >> publish_events
publish_events >> finish_pipeline