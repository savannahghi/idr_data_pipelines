import requests
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow import models
from airflow.operators.dummy_operator import DummyOperator

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")

LOCATION = models.Variable.get("LOCATION")

webhook = models.Variable.get("mattermost_webhook")


def mattermost_alert(context, notifications_webhook=webhook):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    logs_url = context.get("task_instance").log_url
    headers = {}
    message = '{"text": "The DAG: '+str(dag_id)+' has failed on task: '+str(task_id)+' Logs can be viewed on: '+str(logs_url)+'"}'
    response = requests.post(notifications_webhook, headers=headers, data=message)
    return response

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': mattermost_alert
}

'''Define the DAG'''

with DAG("idr_pubsub_test", start_date=dt(2022, 9, 5),
        # Not scheduled, trigger only
        schedule_interval=None, 
        default_args=default_args) as dag:

    '''Define the tasks, which will be conducted once the DAG has been triggered by a 
    PubSub Topic through the subscription. The trigger is connected via a Cloud Function'''

    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )
    
    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        )

start_pipeline >> finish_pipeline