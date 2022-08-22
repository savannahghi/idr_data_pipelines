# ABOUT

This folder contains code deployed to Google Cloud Functions (1st Gen) as the link between Google Pub/Sub and 
Airflow on Google Cloud Composer for the Integrated Data Repository (IDR) project. The purpose of the cloud function
is to enable the triggering of the IDR DAG on Cloud Composer in order to orchestrate the ETL workflow. It's process
involves receiving events from a Pub/Sub subscription to the IDR topic and eventually, when an event is successful,
trigger the IDR DAG on Cloud Composer.

## Getting Started

There are a couple of steps to follow before deploying the cloud function:

1. On Google Cloud Console, navigate to Cloud Functions
2. Select Create Function
3. Fill in the **Basics** section with preferred configurations
4. In the **Trigger** section, select trigger type as *Cloud Pub/Sub* and topic as the topic to subscribe to
5. Under **Runtime, build, connections and security settings**, **Runtime**, **Runtime service account** select *Compute Engine default service account*
6. Click Next to proceed to the **Code** section

## Prerequisites

Python 3.10 is used for this code. Once the development environment is accessed, two files already exist i.e. a main.py
file and a requirements.txt file. To enable authorization and communication between the different Google Products used,
in the requirements.txt file input the following.

```requirements.txt
google-auth==2.6.2
requests==2.27.1
```

## Workflow

### Cloud Function to Cloud Composer

Create a new python file that will contain the code that triggers the Cloud Composer DAG. This code can be found on the composer2_airflow_rest_api.py file in this directory. Of importance is the Airflow web_server_url which needs to be parsed to this code as an argument. This url can be found by visiting the Airflow web UI from the Cloud Composer Environment or by running the following in the Command Line:

```bash
gcloud composer environments describe example-environment \
--location=your-composer-region \
--format="value(config.airflowUri)"
```

### Pub/Sub to Cloud Function

In the main.py file, import the composer2_airflow_rest_api and define the **get_event()** that listens to the subscription and receives the event payload. Once received, call the trigger_dag function from composer2_airflow_rest_api in order to trigger the DAG. The Airflow web_server_url is also required here.

On the entrypoint bar, define the **get_event()** function as the entrypoint to be invoked once events from the topic are received by the subscription.


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://choosealicense.com/licenses/mit/)