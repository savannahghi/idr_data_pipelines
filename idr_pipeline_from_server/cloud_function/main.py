'''
Trigger a DAG in a Cloud Composer 2 environment in response to a pubsub event,
using Cloud Functions.
'''

from typing import Any
from google.cloud import bigquery
import composer2_airflow_rest_api
import base64, json, sys, os

'''This is the entrypoint defined on the Cloud Function entrypoint bar'''

def get_event(event, context):
    '''Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    '''
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = pubsub_message.replace("'",'"')
    web_server_url = (
        ["INSERT_AIRFLOW_WEB_SERVER_ADDRESS"]
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = ['INSERT_DAG_ID']

    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, event)
    pubsub_to_bq(['INSERT_DATASET_ID'], ['INSERT_TABLE_ID'], 
        json.loads(pubsub_message))
    print(pubsub_message)

def pubsub_to_bq(dataset_id, table_id, doc):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    errors = client.insert_rows(table, [doc])
    if errors != [] :
      print(errors, file=sys.stderr)
