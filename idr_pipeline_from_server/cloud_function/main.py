'''
Trigger a DAG in a Cloud Composer 2 environment in response to a pubsub event,
using Cloud Functions.
'''

from typing import Any
import composer2_airflow_rest_api
import base64

'''This is the entrypoint defined on the Cloud Function entrypoint bar'''

def get_event(event, context):
    '''Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    '''
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    
    web_server_url = (
        ["INSERT_AIRFLOW_WEB_SERVER_ADDRESS"]
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = ['INSERT_DAG_ID']

    composer2_airflow_rest_api.trigger_dag(web_server_url, dag_id, event)

