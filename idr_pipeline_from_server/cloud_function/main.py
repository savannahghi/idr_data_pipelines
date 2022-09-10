'''
Trigger a DAG in a Cloud Composer 2 environment in response to a pubsub event,
using Cloud Functions.
'''
import json
import ast
import base64
import sys
import os

from typing import Any
from google.cloud import bigquery

import composer2_airflow_rest_api

AIRFLOW_URL = os.environ.get("airflow_webserver_url")
DAG_ID = os.environ.get("DAG_ID")
BQUERY_DATASET = os.environ.get("metadata_dataset")
BQUERY_TABLE = os.environ.get("metadata_table")

''' This is the entrypoint defined on the Cloud Function entrypoint bar '''
def get_event(event, context):
    ''' Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    '''
    pubsub_message = ast.literal_eval(
        base64.b64decode(event['data']).decode('utf-8')
    )
    pubsub_to_bq(BQUERY_DATASET, BQUERY_TABLE, pubsub_message)
    composer2_airflow_rest_api.trigger_dag(AIRFLOW_URL, DAG_ID, event)

def pubsub_to_bq(dataset_id, table_id, doc):
    ''' Pushes Metadata from event to BigQuery Table
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
