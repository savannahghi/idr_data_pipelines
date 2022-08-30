'''
Trigger a DAG in a Cloud Composer 2 environment in response to a pubsub event,
using Cloud Functions.
'''

from typing import Any
from google.cloud import bigquery
import composer2_airflow_rest_api
import base64, json, sys, os
import ast

webserver = os.environ.get("airflow_webserver_url")
DAG_ID = os.environ.get("DAG_ID")
dataset = os.environ.get("metadata_dataset")
table = os.environ.get("metadata_table")

'''This is the entrypoint defined on the Cloud Function entrypoint bar'''
def get_event(event, context):
    '''Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    '''
    web_server_url = webserver
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = pubsub_message.replace("'",'"')
    message_dict = ast.literal_eval(pubsub_message)
    extract_type = message_dict['extract_type']

    if extract_type == "MMD Extracts from KenyaEMR":
        # Replace with the ID of the DAG that you want to run.
        dag_id = DAG_ID
        composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, event)
        pubsub_to_bq(dataset, table, json.loads(pubsub_message))

def pubsub_to_bq(dataset_id, table_id, doc):
    '''Pushes Metadata from event to BigQuery Table
    Args:
        dataset_id (str): Dataset on BigQuery containing the desired table.
        table_id (str): The desired table where the data will be pushed
        doc (list): Contents to be pushed, in json format
    '''
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)
    errors = client.insert_rows(table, [doc])
    if errors != [] :
      print(errors, file=sys.stderr)
