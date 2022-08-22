# ABOUT

This folder contains code deployed to Google Cloud Composer for the Integrated Data Repository (IDR) project. This is the environment that orchestrates the ETL workflow of the project. It's broken down into two DAGS, the first (airflow_idr_pub.py) contains the loading workflow from the Google Cloud Storage (GCS) bucket to the Google BigQuery (BQ) staging area. The second (airflow_idr_bq.py) contains the transformations of the data and availing the data in the Data Warehouse from where the Visualizations can be done with tools such as Microsoft's PowerBI.


## Workflow

The purpose of separating the two workflows is to reduce the costs of querying the entire pipeline everytime an upload from different facilities is uploaded into the GCS bucket. Due to connectivity issues and availability of servers from the facilities, the anticipation is that the data received by the IDR server from different facilities, will arrive at different times of the day, therefore, the *Transformations pipeline* is set to run at 6 a.m daily, whereas the *Loading pipeline* will run everytime there is a new upload into the bucket from the IDR server.

### airflow_idr_pub.py

Before creating the DAG, a couple of configurations are required:

```
GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= ["INSERT_PROJECT_ID"]
MMD_BUCKET_NAME = ["INSERT_BUCKET_NAME"]
STAGING_DATASET = ["INSERT_STAGING_DATASET"]
LOCATION = ["INSERT_LOCATION"]
DAG_ID =  ['INSERT_DAG_ID']
```

The purpose of this DAG is to load the data from the bucket to the staging area.

### airflow_idr_bq.py

Before creating the DAG, a couple of configurations are required:

```
GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= ["INSERT_PROJECT_ID"]
MMD_STAGING_DATASET = ["INSERT_STAGING_DATASET_NAME"]
DAG_ID = ['INSERT_DAG_ID']
LOCATION = ["INSERT_LOCATION"]
```

Additionally, the **default_args** need to be specified on top of those already defined:

```
'owner': ['INSERT_OWNER_NAME'],
'depends_on_past': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
'email_on_failure': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
'email_on_retry': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
```

The DAG defines multiple Transformations, however, they fall into 4 main categories:

1. Deduplicating the Data.
2. Cleaning the Data.
3. Enriching the Data.
4. Storing the Data in the Data Warehouse.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)