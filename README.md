# IDR DATA PIPELINE

The IDR(Integrated Data Repository) Data Pipeline is the final bit of the IDR workflow that transforms data and stores it in a Data Warehouse on Google BigQuery. For Further Context see [IDR_Client](https://github.com/savannahghi/idr-client) and [IDR_Server](https://github.com/savannahghi/idr-server) for the initial step of extraction of the ETL process. This section focuses on Loading data received in the Data Lake (Google Cloud Storage Bucket) into the Staging Area (Google BigQuery), from where Transformation of the data occurs before finally being stored in a Warehouse (also on Google BigQuery). 

## Description

This project contains procedures followed to complete the automation of the Integrated Data Repository (IDR) for the Fahari Ya Jamii (FYJ) programme conducted by the team at Savannah Informatics. The data is collected from relevant hospitals’ KenyaEMR instances through a client developed by the team. It is sent to a server developed by the team, from where the server sends the data to a DataLake hosted on a Google Cloud Storage Bucket. The server sends metadata information about the data sent to a PubSub Topic from which airflow integration is used as a Subscription to the topic to trigger the workflow orchestration of loading the data, cleaning and transforming, and eventually having it available in a Warehouse on Google BigQuery.

This documentation is only relevant to the final steps that include workflow integration on the Google Cloud Platform (GCP) products, where the workflow orchestration products in use include Google Cloud Composer and Pub/Sub. A Cloud Function has also been used to integrate the communication between Pub/Sub and Cloud Composer to enable the triggering of the DAG once new data has been transmitted to the Cloud Storage Bucket.

The Data Pipeline model will contain the following ELT steps orchestrated by Airflow on Cloud Composer:
1. Loading the Data From Google Cloud Storage to Google Bigquery.
2. Transforming the Data on Bigquery.
3. Enriching the Data on Bigquery.
4. Storing the Data on Bigquery.


## Usage

The use of this pipeline is orchestrated by two main tools which are products of the Google Cloud Platform. The first is **Google Cloud Functions**, which "listens" to the messages from **Google Cloud Pub/Sub** once an event occurs. This Function triggers the next tool, which is an Airflow DAG hosted on **Google Cloud Composer**, that Loads the Data into **Google BigQuery** from the relevent **Google Cloud Storage** Bucket for Data Transformations and Storage in the Warehouse. 

For relevant code and documentation regarding the two primary tools see:
1. [cloud_function](https://github.com/savannahghi/idr_data_pipelines/tree/main/idr_pipeline_from_server/cloud_function)
2. [dags](https://github.com/savannahghi/idr_data_pipelines/tree/main/idr_pipeline_from_server/dags)

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)