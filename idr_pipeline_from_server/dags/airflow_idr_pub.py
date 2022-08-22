'''Import Required Libraries'''

import datetime
import airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

'''Define Global Variables'''

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= ["INSERT_PROJECT_ID"]
MMD_BUCKET_NAME = ["INSERT_BUCKET_NAME"]
STAGING_DATASET = ["INSERT_STAGING_DATASET"]
LOCATION = ["INSERT_LOCATION"]
DAG_ID =  ['INSERT_DAG_ID']

'''Define the DAG'''

with airflow.DAG(
        DAG_ID,
        start_date=datetime.datetime(2022, 8, 16),
        # Not scheduled, trigger only
        schedule_interval=None) as dag:

    '''Define the tasks, which will be conducted once the DAG has been triggered by a 
    PubSub Topic through the subscription. The trigger is connected via a Cloud Function'''

    load_dataset_MMD = GCSToBigQueryOperator(
        task_id = 'load_dataset_MMD',
        bucket = MMD_BUCKET_NAME,
        source_objects = ['*'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.staging_MMD',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'parquet',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields = [
        {"name": "DOB", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Gender", "type": "STRING", "mode": "NULLABLE"},
        {"name": "CCC", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PatientPK", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeEnrollment", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeARTStart", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "AgeLastVisit", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "SiteCode", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "FacilityName", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RegistrationDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "PatientSource", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PreviousARTStartDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "StartARTAtThisFAcility", "type": "DATE", "mode": "NULLABLE"},
        {"name": "StartARTDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "PreviousARTUse", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "PreviousARTPurpose", "type": "STRING", "mode": "NULLABLE"},
        {"name": "PreviousARTRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "DateLastUsed", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StartRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "StartRegimenLine", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LastARTDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "LastRegimen", "type": "STRING", "mode": "NULLABLE"},
        {"name": "LastRegimenLine", "type": "STRING", "mode": "NULLABLE"},
        {"name": "ExpectedReturn", "type": "DATE", "mode": "NULLABLE"},
        {"name": "LastVisit", "type": "DATE", "mode": "NULLABLE"},
        {"name": "Duration", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ExitDate", "type": "DATE", "mode": "NULLABLE"},
        {"name": "ExitReason", "type": "STRING", "mode": "NULLABLE"},
        {"name": "Date_Created", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "Date_Last_Modified", "type": "TIMESTAMP", "mode": "NULLABLE"},
        ]
        )

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        )

'''Define the task dependencies'''

load_dataset_MMD >> finish_pipeline