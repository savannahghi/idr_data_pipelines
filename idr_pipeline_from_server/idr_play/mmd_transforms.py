import requests
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")
STAGING_DATASET = models.Variable.get("MMD_play")
LOCATION = models.Variable.get("LOCATION")
WAREHOUSE = models.Variable.get("IDR_play")
MFL = models.Variable.get("mfl")

def alert():
    import mattermost
    a = mattermost.mattermost_alert()
    return a 

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'start_date':  datetime(2022,9,9),
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': alert

}

with DAG('keemr_mmd_transforms__play', schedule_interval='0 3 * * *', default_args=default_args) as dag:

    ''' 
        Use ExternalTaskSensor to listen to the idr_load_stage_play DAG and finish_pipeline task
        when finish_pipeline is finished, keemr_mmd_transforms_play will be triggered
    '''

    # listener = ExternalTaskSensor(
    #     task_id='waiting_task',
    #     external_dag_id='idr_load_stage__play',
    #     external_task_id='finish_pipeline',
    #     mode = 'reschedule',
    #     timeout=3600,
    # )

    data_types = BigQueryOperator(
        task_id='assign_appropriate_data_types',
        sql=f'''
        SELECT cast(DOB as DATE) as DOB, Gender, cast(weight as FLOAT64) as weight, cast(height as FLOAT64) as height, CCC, 
        cast(PatientPK as INT) as PatientPK, NationalID, cast(AgeEnrollment as FLOAT64) as AgeEnrollment, 
        cast(AgeARTStart as FLOAT64) as AgeARTStart, cast(AgeLastVisit as FLOAT64) as AgeLastVisit, cast(SiteCode as INT) as SiteCode, 
        FacilityName, cast(RegistrationDate as DATE) as RegistrationDate, PatientSource, 
        cast(PreviousARTStartDate as DATE) as PreviousARTStartDate, cast(StartARTAtThisFAcility as DATE) as StartARTAtThisFAcility, 
        cast(StartARTDate as DATE) as StartARTDate, PreviousARTUse, PreviousARTPurpose, PreviousARTRegimen, DateLastUsed, StartRegimen, 
        StartRegimenLine, cast(LastARTDate as DATE) as LastARTDate, LastRegimen, LastRegimenLine, cast(ExpectedReturn as DATE) as ExpectedReturn, 
        cast(LastVisit as DATE) as LastVisit, cast(Duration as FLOAT64) as Duration, cast(ExitDate as DATE) as ExitDate, ExitReason, 
        cast(Date_Created as TIMESTAMP) as Date_Created, cast(Date_Last_Modified as TIMESTAMP) as Date_Last_Modified
        FROM `{PROJECT_ID}.{STAGING_DATASET}.staging`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    deduplicate_ART = BigQueryOperator(
        task_id='deduplicate_ART',
        sql=f'''
        SELECT DISTINCT * FROM
        (SELECT max(DOB) as DOB, max(Gender) as Gender, max(weight) as weight, max(height) as height, CCC, max(PatientPK) as PatientPK, 
        max(NationalID) as NationalID, max(AgeEnrollment) as AgeEnrollment, max(AgeARTStart) as AgeARTStart, max(AgeLastVisit) as AgeLastVisit, 
        SiteCode, max(FacilityName) as FacilityName, max(RegistrationDate) as RegistrationDate, max(PatientSource) as PatientSource, 
        max(cast(PreviousARTStartDate as DATE)) as PreviousARTStartDate, max(StartARTAtThisFAcility) as StartARTAtThisFAcility, 
        max(cast(StartARTDate as DATE)) as StartARTDate, max(PreviousARTUse) as PreviousARTUse, max(PreviousARTPurpose) as PreviousARTPurpose, 
        max(PreviousARTRegimen) as PreviousARTRegimen, max(DateLastUsed) as DateLastUsed, max(StartRegimen) as StartRegimen, 
        max(StartRegimenLine) as StartRegimenLine, max(LastARTDate) as LastARTDate, max(LastRegimen) as LastRegimen, 
        max(LastRegimenLine) as LastRegimenLine, max(ExpectedReturn) as ExpectedReturn, max(LastVisit) as LastVisit, max(Duration) as Duration, 
        max(ExitDate) as ExitDate, max(ExitReason) as ExitReason, max(Date_Created) as Date_Created, max(Date_Last_Modified) as Date_Last_Modified
        FROM `{PROJECT_ID}.{STAGING_DATASET}.staging`
        GROUP BY SiteCode, CCC)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    return_heirarchy = BigQueryOperator(
        task_id='ART_return_dates_heirarchy',
        sql=f'''
        SELECT *,
        DATE_DIFF(ExpectedReturn, LastARTDate, year) AS years,
        DATE_DIFF(ExpectedReturn, LastARTDate, month) AS months,
        DATE_DIFF(ExpectedReturn, LastARTDate, day) AS days,
        FROM `{PROJECT_ID}.{STAGING_DATASET}.deduplicate`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dates_heirarchy',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    clean_regimen = BigQueryOperator(
        task_id='clean_regimen_lines',
        sql=f'''
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
        FROM `{PROJECT_ID}.{STAGING_DATASET}.dates_heirarchy`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.regimens',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    date_visit = BigQueryOperator(
        task_id='date_enrichment',
        sql=f'''
        SELECT *, ExpectedReturn AS DateExpected
        FROM `{PROJECT_ID}.{STAGING_DATASET}.regimens`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dates_enrichment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr = BigQueryOperator(
        task_id='current_on_treatment_enrichment',
        sql=f'''
        SELECT *,
        DATE_DIFF(CURRENT_DATE("UTC"), DateExpected, day) AS CurrentDays
        FROM `{PROJECT_ID}.{STAGING_DATASET}.dates_enrichment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.current_days',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr2 = BigQueryOperator(
        task_id='further_current_on_treatment_enrichment',
        sql=f'''
        SELECT *, CASE
        WHEN CurrentDays < 31 AND LossOfLife = 0 THEN "Yes"
        ELSE "NO"
        END AS CurrentOnTreatment
        FROM 
        (SELECT *, CASE
        WHEN ExitReason = "Died" THEN 1
        ELSE 0
        END AS LossOfLife FROM `{PROJECT_ID}.{STAGING_DATASET}.current_days`)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl_ART = BigQueryOperator(
        task_id='ART_joining_MFL_Codes',
        sql=f'''
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.constituency_name, MFL_Codes.sub_county_name,
        MFL_Codes.ward_name, MFL_Codes.lat, MFL_Codes.long, Staging.DOB, Staging.Gender, Staging.CCC as PatientID,
        Staging.PatientPK, Staging.weight, Staging.height, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, 
        Staging.FacilityName, Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
        Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
        Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, 
        Staging.LastVisit, Staging.Duration, Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, 
        Staging.months, Staging.days,Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, 
        Staging.CurrentOnTreatment
        FROM `{PROJECT_ID}.{MFL}.MFL_Codes` as MFL_Codes
        INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.Tx_Curr` as Staging
        ON MFL_Codes.SiteCode = cast(Staging.SiteCode as INT)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    dates_ART = BigQueryOperator(
        task_id='ART_enriching_joined_table',
        sql=f'''
        SELECT *,
        FORMAT_DATETIME("%Y", LastARTDate) AS LastARTYear,
        FORMAT_DATETIME("%B", LastARTDate) AS LastARTMonth,
        EXTRACT(DAY FROM LastARTDate) AS LastARTDay,
        FORMAT_DATETIME("%Y", cast(StartARTDate as DATE)) AS StartARTYear,
        FORMAT_DATETIME("%B", cast(StartARTDate as DATE)) AS StartARTMonth,
        EXTRACT(DAY FROM cast(StartARTDate as DATE)) AS StartARTDay,
        FROM `{PROJECT_ID}.{STAGING_DATASET}.Tx_Curr`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    hubs_ART = BigQueryOperator(
        task_id='hub_details',
        sql=f'''
        SELECT Staging.SiteCode, Staging.county_name, Staging.constituency_name, Staging.sub_county_name,
        Staging.ward_name, Staging.lat, Staging.long, Staging.DOB, Staging.Gender, Staging.PatientID,
        Staging.PatientPK, Staging.weight, Staging.height, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, 
        Staging.FacilityName, Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
        Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
        Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, 
        Staging.LastVisit, Staging.Duration, Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, 
        Staging.months, Staging.days, Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, 
        Staging.CurrentOnTreatment, Staging.LastARTYear, Staging.LastARTMonth, Staging.LastARTDay, Staging.StartARTYear, Staging.StartARTMonth, 
        Staging.StartARTDay, Hub.Hub
        FROM `{PROJECT_ID}.{MFL}.hub_details` as Hub
        INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.Tx_Curr` as Staging
        ON Staging.SiteCode = Hub.MFL_Code
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.Tx_Curr',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    warehouse_ART = BigQueryOperator(
        task_id='ART_MMD_data_warehouse',
        sql=f'''
        SELECT DISTINCT * FROM `{PROJECT_ID}.{STAGING_DATASET}.Tx_Curr`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.art_mmd',
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

# listener >> 
data_types >> deduplicate_ART >> return_heirarchy >> clean_regimen >> date_visit >> tx_curr
date_visit >> tx_curr >> tx_curr2 >> mfl_ART >> dates_ART >> hubs_ART >> warehouse_ART >> finish
