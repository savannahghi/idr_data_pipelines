# ABOUT

This folder contains code deployed to Google Cloud Composer for the Integrated Data Repository (IDR) project. This is the environment that orchestrates the ETL workflow of the project. It's broken down into three DAG groups, the first (*idr_pubsub_test*) is triggered every time there's an event to notify thet there are new extracts. The second (*airflow_idr_pub.py*) contains the loading workflow from the Google Cloud Storage (GCS) bucket to the Google BigQuery (BQ) staging area. The final (*airflow_idr_bq.py*) dag contains the transformations of the data and availing the data in the Data Warehouse from where the Visualizations can be done with tools such as Microsoft's PowerBI.


## Workflow

The purpose of separating the three workflows is to reduce the costs of querying the entire pipeline everytime an upload from different facilities is uploaded into the GCS bucket. Due to connectivity issues and availability of servers from the facilities, the anticipation is that the data received by the IDR server from different facilities, will arrive at different times of the day, therefore, the *Transformations pipeline* is set to run daily (at 6 a.m), whereas the *Loading pipeline* will run everytime there is a new upload into the bucket from the IDR server.

### airflow_idr_pub.py

Before creating the DAG, a couple of environment variables are required.
These are set via the Airflow Webserver UI's **Admin** Tab, on the **Variables** option.
These Variables can then be accessed on the required DAG as follows:

```
PROJECT_ID = models.Variable.get("PROJECT_ID")
BUCKET_NAME = models.Variable.get("test_bucket")
STAGING_DATASET = models.Variable.get("IDR_test")
LOCATION = models.Variable.get("LOCATION")
```

Additionally, the **default_args** need to be specified on top of those already defined:

```
'owner': 'SGHI',
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'on_failure_callback': mattermost_alert
```

In case of a failure, a custom function **mattermost_alert** has been created which will be called and executed to send a notification to the project's mattermost channel with a link to the Airflow Webserver's Logs url. This will be the link to the exact log from the exact failed task for the purpose of troubleshooting.

The purpose of this DAG is to load the data from the bucket to the staging area. After this is done, it will send a message to Pub/Sub with details of the new successful messages received.

### airflow_idr_bq.py

Before creating the DAG, a couple of configurations are required:

```
PROJECT_ID = models.Variable.get("PROJECT_ID")
STAGING_DATASET = models.Variable.get("programe_staging_test")
WAREHOUSE = models.Variable.get("IDR_test")
LOCATION = models.Variable.get("LOCATION")
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