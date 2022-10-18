# IDR DATA PIPELINE

The IDR(Integrated Data Repository) Data Pipeline is the final bit of the IDR workflow that transforms data and stores it in a Data Warehouse on Google BigQuery. For Further Context see [IDR_Client](https://github.com/savannahghi/idr-client) and [IDR_Server](https://github.com/savannahghi/idr-server) for the initial step of extraction of the ETL process. This section focuses on Loading data received in the Data Lake (Google Cloud Storage Bucket) into the Staging Area (Google BigQuery), from where Transformation of the data occurs before finally being stored in a Warehouse (also on Google BigQuery). 

## Description

This project contains procedures followed to complete the automation of the Integrated Data Repository (IDR) for the Fahari Ya Jamii (FYJ) programme. The data is collected from relevant hospitals’ KenyaEMR instances through a client developed by the team. It is sent to a server, also developed by the team, from where the server sends the data to a DataLake hosted on a Google Cloud Storage Bucket. The server sends metadata information about the data sent to a PubSub Topic from which airflow integration is used as a Subscription to the topic to trigger the workflow orchestration of loading the data, cleaning and transforming, and eventually having it available in a Warehouse on Google BigQuery.

This documentation is only relevant to the final steps that include workflow integration on the Google Cloud Platform (GCP) products, where the workflow orchestration products in use include Google Cloud Composer and Pub/Sub. A Cloud Function has also been used to integrate the communication between Pub/Sub and Cloud Composer to enable the triggering of the DAG once new data has been transmitted to the Cloud Storage Bucket.

The Data Pipeline model will contain the following ELT steps orchestrated by Airflow on Cloud Composer:
1. Loading the Data From Google Cloud Storage to Google Bigquery.
2. Transforming the Data on Bigquery.
3. Enriching the Data on Bigquery.
4. Storing the Data on Bigquery.


## Usage

The use of this pipeline is orchestrated by two main tools which are products of the Google Cloud Platform. The first is **Google Cloud Functions**, which "listens" to the messages from **Google Cloud Pub/Sub** once an event occurs. This Function triggers the next tool, which is an Airflow DAG hosted on **Google Cloud Composer**. 

For relevant code regarding the two primary tools see:
1. [cloud_function](https://github.com/savannahghi/idr_data_pipelines/tree/main/idr_pipeline_from_server/cloud_function)
2. [dags](https://github.com/savannahghi/idr_data_pipelines/tree/main/idr_pipeline_from_server/dags)

### 1. CLOUD FUNCTION

The purpose of the cloud function is to enable the triggering of the IDR DAG on Cloud Composer in order to orchestrate the ETL workflow. It's process involves receiving events from a Pub/Sub subscription to the IDR topic and eventually, when an event is successful, trigger the IDR DAG on Cloud Composer.

#### Getting Started

There are a couple of steps to follow before deploying the cloud function:

1. On Google Cloud Console, navigate to Cloud Functions
2. Select Create Function
3. Fill in the **Basics** section with preferred configurations
4. In the **Trigger** section, select trigger type as *Cloud Pub/Sub* and topic as the topic to subscribe to
5. Under **Runtime, build, connections and security settings**, **Runtime**, **Runtime service account** select *Compute Engine default service account*
6. Provide Runtime variables that will be accessed by your code.
7. Click Next to proceed to the **Code** section

#### Prerequisites

The variables specified at runtime for the purpose of secrecy and security are:
* DAG_ID -> DAG ID of the DAG to be triggered
* airflow_webserver_url -> Webserver url of the Airflow instance
* metadata_dataset -> BigQuery Dataset that contains the table where events payload are published from Pub/Sub
* metadata_table -> BigQuery Table where events payload are published from Pub/Sub

#### Workflow

##### *Cloud Function to Cloud Composer*

Create a new python file that will contain the code that triggers the Cloud Composer DAG. This code can be found on the *composer2_airflow_rest_api.py* file. Of importance is the *Airflow web_server_url* which needs to be parsed to this code as an argument. This url can be found by visiting the Airflow web UI from the Cloud Composer Environment or by running the following in the Command Line:

```bash
gcloud composer environments describe example-environment \
--location=your-composer-region \
--format="value(config.airflowUri)"
```

##### *Pub/Sub to Cloud Function*

In the *main.py* file, import the *composer2_airflow_rest_api* and define the **get_event()** function that listens to the subscription and receives the event payload. Once received, call the *trigger_dag* function from *composer2_airflow_rest_api* in order to trigger the DAG. The Airflow web_server_url is also required here.

On the entrypoint bar, define the **get_event()** function as the entrypoint to be invoked once events from the topic are received by the subscription.

### 2.DAGs

This section contains code deployed to Google Cloud Composer for the Integrated Data Repository (IDR) project. This is the environment that orchestrates the ETL workflow of the project. It's broken down into three DAG groups, the first (*idr_pubsub*) is triggered every time there's an event to notify that there are new extracts. The second (*idr_load.py*) contains the loading workflow from the Google Cloud Storage (GCS) bucket to the Google BigQuery (BQ) staging area. The final dag group contains dags that transform the data, each unique to the extract type, and availing the data in the Data Warehouse from where the Visualizations can be done with tools such as Microsoft's PowerBI.

#### Workflow

The purpose of breaking down the workflows is to reduce the costs of querying the entire pipeline everytime an upload from different facilities is stored in the GCS bucket. Due to connectivity issues and availability of servers from the facilities, the anticipation is that the data received by the IDR server from different facilities, will arrive at different times of the day, therefore, the *Loading pipeline*  and *Transformations pipelines* are set to run daily (at 7 a.m), whereas the *Pub/Sub Events pipeline* will run everytime there is a new upload into the bucket from the IDR server. The *Transformations pipelines* have **External Task Sensors** that listen to the **finish_pipeline** task of the *Loading pipeline* which triggers their specific workflows simultaneously. The exception is **vls_transforms** that is triggered by the **finish_pipeline** task of *mmd_transforms*.

##### *idr_pubsub*

This DAG acts as a liveness monitoring system for the Pub/Sub messages being sent to a BigQuery Table. It makes it easier to assess the last run as compared to logs on Pub/Sub.

##### *idr_load*

Before creating the DAG, a couple of environment variables are required.
These are set via the Airflow Webserver UI's **Admin** Tab, on the **Variables** option.
These Variables can then be accessed on the required DAG as follows:

```
PROJECT_ID 
BUCKET_NAME 
STAGING_DATASET 
LOCATION 
mattermost_webhook
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

The purpose of this DAG is to load the data from the bucket to the staging area.

##### *transformations*

Before creating the DAG, on top of the variables defined in the loading DAG, the following is required:

```
WAREHOUSE
```

The DAG defines multiple Transformations, however, they fall into 4 main categories:

1. Deduplicating the Data.
2. Cleaning the Data.
3. Enriching the Data.
4. Storing the Data in the Data Warehouse.

Each *extract type* i.e. COVID, HTS(HIV Testing), MMD(Multi-Month Dispensing) and VLS(Viral Load Suppression) has it's own Transformation process due to the different data they contain. As mentioned before, these transformations will be triggered once the loading data pipeline is finished. MMD transformations are triggered first. Once complete, The VLS transformations pipeline will be triggered. This is because VLS and MMD data contain a cascade from which VLS data is required for patients on MMD. HTS transformations will then be triggered and finally COVID.

For each *extract type's* transformations, the code has descriptive naming and DAG task ID's to explain the transformation done by each task.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)
