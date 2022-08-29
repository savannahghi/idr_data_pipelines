import requests
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")
ART_STAGING_DATASET = models.Variable.get("MMD_test")
LOCATION = models.Variable.get("LOCATION")
WAREHOUSE = models.Variable.get("IDR_test")
webhook = models.Variable.get("mattermost_webhook")

def mattermost_alert(context, notifications_webhook=webhook):
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    logs_url = context.get("task_instance").log_url
    headers = {}
    message = '{"text": "The DAG: '+str(dag_id)+' has failed on task: '+str(task_id)+' Logs can be viewed on: '+str(logs_url)+'"}'
    response = requests.post(notifications_webhook, headers=headers, data=message)
    return response

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'start_date':  datetime(2022,8,22),
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': mattermost_alert
}

with DAG('idr_transforms_test', schedule_interval='0 6 * * *', default_args=default_args) as dag:

    deduplicate_ART = BigQueryOperator(
        task_id='deduplicate_ART',
        sql=f'''
        #standardSQL
        SELECT DISTINCT * FROM
        (SELECT max(DOB) as DOB, max(Gender) as Gender, CCC, max(PatientPK) as PatientPK, max(AgeEnrollment) as AgeEnrollment,
        max(AgeARTStart) as AgeARTStart, max(AgeLastVisit) as AgeLastVisit, SiteCode, max(FacilityName) as FacilityName,
        max(RegistrationDate) as RegistrationDate, max(PatientSource) as PatientSource, max(cast(PreviousARTStartDate as DATE)) as PreviousARTStartDate,
        max(StartARTAtThisFAcility) as StartARTAtThisFAcility, max(cast(StartARTDate as DATE)) as StartARTDate,
        max(PreviousARTUse) as PreviousARTUse, max(PreviousARTPurpose) as PreviousARTPurpose, max(PreviousARTRegimen) as PreviousARTRegimen,
        max(DateLastUsed) as DateLastUsed, max(StartRegimen) as StartRegimen, max(StartRegimenLine) as StartRegimenLine,
        max(LastARTDate) as LastARTDate, max(LastRegimen) as LastRegimen, max(LastRegimenLine) as LastRegimenLine, 
        max(ExpectedReturn) as ExpectedReturn, max(LastVisit) as LastVisit, max(Duration) as Duration, max(ExitDate) as ExitDate,
        max(ExitReason) as ExitReason, max(Date_Created) as Date_Created, max(Date_Last_Modified) as Date_Last_Modified
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.staging_MMD`
        GROUP BY SiteCode, CCC)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    return_heirarchy = BigQueryOperator(
        task_id='ART_return_dates_heirarchy',
        sql=f'''
        #standardSQL
        SELECT *,
        DATE_DIFF(ExpectedReturn, LastARTDate, year) AS years,
        DATE_DIFF(ExpectedReturn, LastARTDate, month) AS months,
        DATE_DIFF(ExpectedReturn, LastARTDate, day) AS days,
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_deduplicate`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_dates_heirarchy',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    clean_regimen = BigQueryOperator(
        task_id='clean_regimen_lines',
        sql=f'''
        #standardSQL
        SELECT *,CASE
        WHEN LastRegimenLine = "First line" THEN "1st line"
        WHEN LastRegimenLine = "Second line" THEN "2nd line"
        WHEN LastRegimenLine = "Third line" THEN "3rd line"
        ELSE "Uncategorized"
        END AS LastRegimenLineClean,
        CASE
        WHEN StartRegimenLine = "First line" THEN "1st line"
        WHEN StartRegimenLine = "Second line" THEN "2nd line"
        WHEN StartRegimenLine = "Third line" THEN "3rd line"
        ELSE "Uncategorized"
        END AS StartRegimenLineClean
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_dates_heirarchy`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_regimens',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    date_visit = BigQueryOperator(
        task_id='date_enrichment',
        sql=f'''
        #standardSQL
        SELECT *, ExpectedReturn AS DateExpected
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_regimens`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_dates_enrichment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr = BigQueryOperator(
        task_id='current_on_treatment_enrichment',
        sql=f'''
        #standardSQL
        SELECT *,
        DATE_DIFF(CURRENT_DATE("UTC"), DateExpected, day) AS CurrentDays
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_dates_enrichment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_current_days',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr2 = BigQueryOperator(
        task_id='further_current_on_treatment_enrichment',
        sql=f'''
        #standardSQL
        SELECT *, CASE
        WHEN CurrentDays < 31 AND ExitReason != "Died" THEN "Yes"
        ELSE "NO"
        END AS CurrentOnTreatment
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_current_days`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl_ART = BigQueryOperator(
        task_id='ART_joining_MFL_Codes',
        sql=f'''
        #standardSQL
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.constituency_name, MFL_Codes.sub_county_name,
        MFL_Codes.ward_name, MFL_Codes.lat, MFL_Codes.long, Staging.DOB, Staging.Gender, Staging.CCC as PatientID,
        Staging.PatientPK, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, Staging.FacilityName,
        Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
        Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
        Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, Staging.LastVisit, Staging.Duration,
        Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, Staging.months, Staging.days,
        Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, Staging.CurrentOnTreatment
        FROM `{PROJECT_ID}.MFL.MFL_Codes` as MFL_Codes
        INNER JOIN `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_Tx_Curr` as Staging
        ON MFL_Codes.SiteCode = cast(Staging.SiteCode as INT)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    dates_ART = BigQueryOperator(
        task_id='ART_enriching_joined_table',
        sql=f'''
        #standardSQL
        SELECT *,
        FORMAT_DATETIME("%Y", LastARTDate) AS LastARTYear,
        FORMAT_DATETIME("%B", LastARTDate) AS LastARTMonth,
        EXTRACT(DAY FROM LastARTDate) AS LastARTDay,
        FORMAT_DATETIME("%Y", cast(StartARTDate as DATE)) AS StartARTYear,
        FORMAT_DATETIME("%B", cast(StartARTDate as DATE)) AS StartARTMonth,
        EXTRACT(DAY FROM cast(StartARTDate as DATE)) AS StartARTDay,
        FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_Tx_Curr`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    hubs_ART = BigQueryOperator(
        task_id='hub_details',
        sql=f'''
        #standardSQL
        SELECT Staging.SiteCode, Staging.county_name, Staging.constituency_name, Staging.sub_county_name,
        Staging.ward_name, Staging.lat, Staging.long, Staging.DOB, Staging.Gender, Staging.PatientID,
        Staging.PatientPK, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, Staging.FacilityName,
        Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
        Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
        Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, Staging.LastVisit, Staging.Duration,
        Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, Staging.months, Staging.days,
        Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, Staging.CurrentOnTreatment,
        Staging.LastARTYear, Staging.LastARTMonth, Staging.LastARTDay, Staging.StartARTYear, Staging.StartARTMonth, Staging.StartARTDay,
        Hub.Hub
        FROM `{PROJECT_ID}.MFL.hub_details` as Hub
        INNER JOIN `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_Tx_Curr` as Staging
        ON Staging.SiteCode = Hub.MFL_Code
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{ART_STAGING_DATASET}.ART_MMD_Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    warehouse_ART = BigQueryOperator(
        task_id='ART_MMD_data_warehouse',
        sql=f'''
        #standardSQL
        SELECT DISTINCT * FROM `{PROJECT_ID}.{ART_STAGING_DATASET}.ART_MMD_Tx_Curr`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.ART_MMD',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )



# ART_MMD Pipeline
deduplicate_ART >> return_heirarchy >> clean_regimen >> date_visit >> tx_curr
tx_curr >> tx_curr2 >> mfl_ART >> dates_ART >> hubs_ART >> warehouse_ART
