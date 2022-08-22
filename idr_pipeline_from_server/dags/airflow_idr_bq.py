import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

'''Define Global Variables'''
GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= ["INSERT_PROJECT_ID"]
MMD_STAGING_DATASET = ["INSERT_STAGING_DATASET_NAME"]
DAG_ID = ['INSERT_DAG_ID']
LOCATION = ["INSERT_LOCATION"]


default_args = {
    'owner': ['INSERT_OWNER_NAME'],
    'depends_on_past': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
    'email_on_failure': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
    'email_on_retry': ['BOOLEAN VALUE, EITHER TRUE OR FALSE'],
    'retries': 2,
    'start_date':  datetime(2022,8,17),
    'retry_delay': timedelta(minutes=3),
}

with DAG(DAG_ID, schedule_interval='0 6 * * *', default_args=default_args) as dag:

    deduplicate_ART = BigQueryOperator(
        task_id='deduplicate_ART',
        sql='''
        #standardSQL
        SELECT DISTINCT * from `fyj-342011.IDR_test.staging_MMD`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_deduplicate',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    return_heirarchy = BigQueryOperator(
        task_id='ART_return_dates_heirarchy',
        sql='''
        #standardSQL
        SELECT *,
        DATE_DIFF(ExpectedReturn, LastARTDate, year) AS years,
        DATE_DIFF(ExpectedReturn, LastARTDate, month) AS months,
        DATE_DIFF(ExpectedReturn, LastARTDate, day) AS days,
        FROM `fyj-342011.IDR_test.staging_ART_MMD_deduplicate`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_MMD',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    clean_regimen = BigQueryOperator(
        task_id='clean_regimen_lines',
        sql='''
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
          FROM `fyj-342011.IDR_test.staging_ART_MMD_MMD`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_MMD_Regimen',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    date_visit = BigQueryOperator(
        task_id='date_enrichment',
        sql='''
        #standardSQL
        SELECT *,
        DATE_ADD(LastVisit, INTERVAL cast(Duration as INT) day) AS DateExpected
        FROM `fyj-342011.IDR_test.staging_ART_MMD_MMD_Regimen`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    tx_curr = BigQueryOperator(
        task_id='current_on_treatment_enrichment',
        sql='''
        #standardSQL
        SELECT *,
        DATE_DIFF(CURRENT_DATE("UTC"), DateExpected, day) AS CurrentDays
        FROM `fyj-342011.IDR_test.staging_ART_MMD_CurrentOnTreatment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    tx_curr2 = BigQueryOperator(
        task_id='further_current_on_treatment_enrichment',
        sql='''
        #standardSQL
        SELECT *, CASE
          WHEN ExitReason IS NOT NULL THEN "NO"
          WHEN CurrentDays > 30 THEN "NO"
          WHEN CurrentDays < 31 THEN "Yes"
          END AS CurrentOnTreatment
          FROM `fyj-342011.IDR_test.staging_ART_MMD_CurrentOnTreatment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.staging_ART_MMD_CurrentOnTreatment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    mfl_ART = BigQueryOperator(
        task_id='ART_joining_MFL_Codes',
        sql='''
        #standardSQL
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.constituency_name, MFL_Codes.sub_county_name,
          MFL_Codes.ward_name, MFL_Codes.lat, MFL_Codes.long, Staging.DOB, Staging.Gender, Staging.CCC as PatientID,
          Staging.PatientPK, Staging.AgeEnrollment, Staging.AgeARTStart, Staging.AgeLastVisit, Staging.FacilityName,
          Staging.RegistrationDate, Staging.PatientSource, Staging.PreviousARTStartDate, Staging.StartARTAtThisFAcility,
          Staging.StartARTDate, Staging.PreviousARTUse, Staging.PreviousARTPurpose, Staging.PreviousARTRegimen, Staging.DateLastUsed,
          Staging.StartRegimen, Staging.StartRegimenLine, Staging.LastARTDate, Staging.LastRegimen, Staging.LastRegimenLine, Staging.ExpectedReturn, Staging.LastVisit, Staging.Duration,
          Staging.ExitDate, Staging.ExitReason, Staging.Date_Created, Staging.Date_Last_Modified, Staging.years, Staging.months, Staging.days,
          Staging.LastRegimenLineClean, Staging.StartRegimenLineClean, Staging.DateExpected, Staging.CurrentDays, Staging.CurrentOnTreatment
          FROM `fyj-342011.MMD.MFL_Codes` as MFL_Codes
          INNER JOIN `fyj-342011.IDR_test.staging_ART_MMD_CurrentOnTreatment` as Staging
          ON MFL_Codes.SiteCode = cast(Staging.SiteCode as INT)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.ART_MMD_Warehouse_test',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    dates_ART = BigQueryOperator(
        task_id='ART_enriching_joined_table',
        sql='''
        #standardSQL
        SELECT *,
            FORMAT_DATETIME("%Y", LastARTDate) AS LastARTYear,
            FORMAT_DATETIME("%B", LastARTDate) AS LastARTMonth,
            EXTRACT(DAY FROM LastARTDate) AS LastARTDay,
            FORMAT_DATETIME("%Y", cast(StartARTDate as DATE)) AS StartARTYear,
            FORMAT_DATETIME("%B", cast(StartARTDate as DATE)) AS StartARTMonth,
            EXTRACT(DAY FROM cast(StartARTDate as DATE)) AS StartARTDay,
        FROM `fyj-342011.IDR_test.ART_MMD_Warehouse_test`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.ART_MMD_Warehouse_test',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    warehouse_ART = BigQueryOperator(
        task_id='ART_MMD_data_warehouse',
        sql='''
        #standardSQL
        select distinct * from `fyj-342011.IDR_test.ART_MMD_Warehouse_test`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{MMD_STAGING_DATASET}.ART_MMD_Warehouse',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )



# ART_MMD Pipeline
deduplicate_ART >> return_heirarchy >> clean_regimen >> date_visit >> tx_curr
tx_curr >> tx_curr2 >> mfl_ART >> dates_ART >> warehouse_ART
