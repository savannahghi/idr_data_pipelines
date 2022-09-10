from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
from io import BytesIO
from airflow import models

project = models.Variable.get("PROJECT_ID")
bucket = models.Variable.get("test_bucket")
folder = models.Variable.get("mmd_folder_test")


def list_blobs_with_prefix(bucket_name, prefix, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        a/1.txt
        a/b/2.txt
    If you specify prefix ='a/', without a delimiter, you'll get back:
        a/1.txt
        a/b/2.txt
    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':
        a/1.txt
    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:
        a/b/
    """

    storage_client = storage.Client()

    """ Note: Client.list_blobs requires at least package version 1.17.0. """
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    blobs_list = []
    for blob in blobs:
        blobs_list.append(blob.name)
    
    return blobs_list

def _get_blob(bucket, path, project):
    storage_client = storage.Client(project=project)
    bucket = storage_client.get_bucket(bucket)
    blob = bucket.blob(path)
    return blob

def get_byte_fileobj(project: str,
                     bucket: str,
                     path: str, ) -> BytesIO:
    """
    Retrieve data from a given blob on Google Storage and pass it as a file object.
    :param path: path within the bucket
    :param project: name of the project
    :param bucket_name: name of the bucket
    :param service_account_credentials_path: path to credentials.
           TIP: can be stored as env variable, e.g. os.getenv('GOOGLE_APPLICATION_CREDENTIALS_DSPLATFORM')
    :return: file object (BytesIO)
    """
    blob = _get_blob(bucket, path, project)
    byte_stream = BytesIO()
    blob.download_to_file(byte_stream)
    byte_stream.seek(0)
    return byte_stream

def make_dataframe(project, bucket_name, prefix):
    blobs_list = list_blobs_with_prefix(bucket_name, prefix)
    df = pd.DataFrame() 
    for b in blobs_list:
        path = b
        print(path)
        file_obj = get_byte_fileobj(project, bucket_name, path)
        df_obj = pd.read_parquet(file_obj)
        df_obj = df_obj.astype(str)
        df = pd.concat([df,df_obj])
    
    df = df.drop_duplicates()
    df = df.reset_index()
    df = df.drop(columns=["index"])
    df = df.fillna(value=np.nan)
    df = df.replace(to_replace=["None"], value=np.nan)

    return df


def load_table_dataframe(table_id: str) -> "bigquery.Table":

    client = bigquery.Client()
    
    df = make_dataframe(project, bucket, folder)

    """ Specify a (partial) schema. All columns are always written to the
            table. The schema is used to assist in data type definitions.
            schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            bigquery.SchemaField("Gender", bigquery.enums.SqlTypeNames.STRING),
            # Indexes are written if included in the schema by name.
            bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
            ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
    """
    job_config = bigquery.LoadJobConfig(

        write_disposition="WRITE_TRUNCATE",
        )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    try:
        job.result()  # Wait for the job to complete.
    except:
        print(job.exception())

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
