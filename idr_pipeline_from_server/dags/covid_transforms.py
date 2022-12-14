import requests
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")
STAGING_DATASET = models.Variable.get("IDR_COVID_test")
LOCATION = models.Variable.get("LOCATION")
WAREHOUSE = models.Variable.get("IDR_test")
MFL = models.Variable.get("MFL")

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'start_date':  dt(2022,9,9),
    'retry_delay': timedelta(minutes=3),
}

with DAG('keemr_covid_transforms_test', schedule_interval='0 4 * * *', default_args=default_args) as dag:

    ''' 
        Use ExternalTaskSensor to listen to the idr_load_stage_test DAG and finish_pipeline task
        when finish_pipeline is finished, keemr_mmd_transforms_test will be triggered
    '''

    listener = ExternalTaskSensor(
        task_id='waiting_task',
        external_dag_id='idr_load_stage_test',
        external_task_id='finish_pipeline',
        mode = 'reschedule',
        timeout=600,
    )

    deduplicate = BigQueryOperator(
        task_id='deduplicate_COVID',
        sql=f'''
        #standardSQL
        SELECT DISTINCT * 
        FROM `{PROJECT_ID}.{STAGING_DATASET}.staging`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl = BigQueryOperator(
        task_id='org_enrichment',
        sql =f'''
        SELECT MFL_Codes.SiteCode, MFL_codes.officialname, MFL_Codes.county_name, MFL_Codes.constituency_name, MFL_Codes.sub_county_name, 
        MFL_Codes.ward_name, MFL_Codes.lat, MFL_Codes.long, Staging.Facilty_Name as Facility_Name, Staging.ccc_number, Staging.phone_number, 
        Staging.id_number, Staging.DOB, Staging.ageInYears, Staging.Gender, Staging.visit_date, Staging.Ever_Vaccinated, Staging.First_Vaccine, 
        Staging.First_Vaccination_Verified, Staging.first_dose_date, Staging.Second_Vaccine, Staging.Second_Vaccination_Verified, 
        Staging.second_dose_date, Staging.Final_Vaccination_Status, Staging.Ever_recieved_Booster, Staging.Booster_Vaccine
        FROM `{PROJECT_ID}.{STAGING_DATASET}.deduplicate` as Staging
        INNER JOIN `{PROJECT_ID}.{MFL}.MFL_Codes` as MFL_Codes
        ON MFL_Codes.SiteCode = cast(Staging.MFL_code as INT)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.org_enrichment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    status_1 = BigQueryOperator(
        task_id='vaccine_status_cleaning',
        sql =f'''
        SELECT *, CASE
        WHEN Final_Vaccination_Status = "Fully Vaccinated" AND Ever_recieved_Booster = "Yes" THEN "Booster Shot"
        ELSE Final_Vaccination_Status
        END AS Vaccination_Final_Status
        FROM `{PROJECT_ID}.{STAGING_DATASET}.org_enrichment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.vaccine_status_cleaning',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    status_2 = BigQueryOperator(
        task_id='vaccine_status_cleaning_2',
        sql =f'''
        SELECT *,CASE
        WHEN Booster_Vaccine is null THEN "Unknown"
        ELSE Booster_Vaccine
        END AS Booster_Vaccine_Type
        FROM
        (SELECT *,CASE
        WHEN Second_Vaccine is null THEN "Unknown"
        ELSE Second_Vaccine
        END AS Second_Vaccine_Type
        FROM
        (SELECT *, CASE
        WHEN First_Vaccine is null THEN "Unknown"
        ELSE First_Vaccine
        END AS First_Vaccine_Type
        FROM `{PROJECT_ID}.{STAGING_DATASET}.vaccine_status_cleaning`))
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.vaccine_status_cleaning',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    warehouse = BigQueryOperator(
        task_id='covid_warehouse',
        sql =f'''
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.vaccine_status_cleaning`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.covid',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    finish = DummyOperator(
        task_id='finish_pipeline',
        dag=dag,
    )

listener >> deduplicate >> mfl >> status_1 >> status_2 >> warehouse >> finish