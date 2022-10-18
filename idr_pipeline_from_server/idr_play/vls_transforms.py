import requests
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")
STAGING_DATASET = models.Variable.get("VLS_play")
LOCATION = models.Variable.get("location")
WAREHOUSE = models.Variable.get("IDR_play")

def alert():
    import sys
    sys.path.append('/home/airflow/gcs/dags/idr_play/dependencies')
    import mattermost
    a = mattermost.mattermost_alert()
    return a 

default_args = {
    'owner': 'SGHI',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'start_date':  dt(2022,9,9),
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': alert
}

with DAG('keemr_vl_transforms_play', schedule_interval=None, default_args=default_args) as dag:

    data_types = BigQueryOperator(
        task_id='assign_appropriate_data_types',
        sql=f'''
        SELECT Mfl_code, cast(patient_id as FLOAT64) as patient_id, ccc_number, Gender, 
        cast(DOB as DATE) as DOB, cast(ageInYears as FLOAT64) as ageInYears, cast(date_test_requested as DATE) as date_test_requested, 
        cast(date_test_result_received as DATE) as date_test_result_received, lab_test, urgency, order_reason, test_result
        FROM `{PROJECT_ID}.{STAGING_DATASET}.staging`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    deduplicate_VLS = BigQueryOperator(
        task_id='deduplicate_COVID',
        sql=f'''
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

    denullification_VLS = BigQueryOperator(
        task_id='denullification_VLS',
        sql=f'''
        SELECT * FROM
        (SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.deduplicate`
        WHERE  ccc_number IS NOT NULL)
        WHERE ((Mfl_code IS NOT NULL) and (ccc_number IS NOT NULL))
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.NULLS',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    viral_load = BigQueryOperator(
        task_id='viral_load_only',
        sql=f'''
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.NULLS`
        WHERE lab_test = "VIRAL LOAD"
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.viral_load',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    latest_date = BigQueryOperator(
        task_id='latest_vl_result',
        sql=f'''
        SELECT MFL_code, ccc_number, MAX(CAST(date_test_result_received as DATE)) AS results_date,
        MAX(CAST(date_test_requested as DATE)) AS date_test_requested, MAX(order_reason) as order_reason,
        MAX(test_result) AS test_result
        FROM `{PROJECT_ID}.{STAGING_DATASET}.viral_load`
        GROUP BY Mfl_code, ccc_number
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.recent_dates',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    single_records = BigQueryOperator(
        task_id='single_patient_records',
        sql=f'''
        SELECT RD.Mfl_code as SiteCode, RD.ccc_number, RD.results_date as vl_results_date, Staging.Gender,
        Staging.DOB, Staging.ageInYears as vl_ageInYears, Staging.date_test_requested as vl_date_test_requested,
        Staging.lab_test as vl_lab_test, Staging.urgency as vl_urgency, Staging.order_reason as vl_order_reason,
        Staging.test_result as vl_test_result
        FROM `{PROJECT_ID}.{STAGING_DATASET}.recent_dates` as RD
        LEFT JOIN `{PROJECT_ID}.{STAGING_DATASET}.viral_load` as Staging
        ON RD.ccc_number = Staging.ccc_number
        AND RD.Mfl_code = Staging.Mfl_code
        WHERE RD.results_date = Staging.date_test_result_received
        AND RD.date_test_requested = Staging.date_test_requested
        AND RD.order_reason = Staging.order_reason
        AND RD.test_result = Staging.test_result
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.patient_single_records',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    VLS_Warehouse = BigQueryOperator(
        task_id='VLS_Warehouse',
        sql=f'''
        SELECT DISTINCT * FROM `{PROJECT_ID}.{STAGING_DATASET}.patient_single_records`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.vls',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    art_vls = BigQueryOperator(
        task_id='merge_art_vls',
        sql=f'''
        SELECT ART.SiteCode, ART.county_name, ART.Hub, ART.constituency_name, ART.sub_county_name, ART.ward_name, ART.lat,
        ART.long, ART.DOB, ART.Gender, ART.PatientID, ART.PatientPK, ART.AgeEnrollment, ART.AgeARTStart, ART.AgeLastVisit,
        ART.FacilityName, ART.RegistrationDate, ART.PatientSource, ART.PreviousARTStartDate, ART.StartARTAtThisFAcility,
        ART.StartARTDate, ART.PreviousARTUse, ART.PreviousARTPurpose, ART.PreviousARTRegimen, ART.DateLastUsed,
        ART.StartRegimen, ART.StartRegimenLine, ART.LastARTDate, ART.LastRegimen, ART.LastRegimenLine, ART.ExpectedReturn,
        ART.LastVisit, ART.Duration, ART.ExitDate, ART.ExitReason, ART.Date_Created, ART.Date_Last_Modified,
        ART.years, ART.months, ART.days, ART.LastRegimenLineClean, ART.StartRegimenLineClean, ART.DateExpected,
        ART.CurrentDays, ART.CurrentOnTreatment, ART.LastARTYear, ART.LastARTMonth, ART.LastARTDay, ART.StartARTYear,
        ART.StartARTMonth, ART.StartARTDay, VLS.vl_results_date, VLS.vl_ageInYears,
        VLS.vl_lab_test, VLS.vl_urgency, VLS.vl_order_reason, VLS.vl_test_result
        FROM `{PROJECT_ID}.{WAREHOUSE}.art_mmd` as ART
        LEFT JOIN `{PROJECT_ID}.{WAREHOUSE}.vls` as VLS
        ON ART.PatientID = VLS.ccc_number
        AND ART.SiteCode = cast(VLS.SiteCode as INT64)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.merge_art_vls',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    valid_tests = BigQueryOperator(
        task_id='valid_results',
        sql=f'''
        SELECT *, CASE
        WHEN days.vl_days_since_test IS NULL THEN "Unknown"
        WHEN days.vl_days_since_test < 366 AND CurrentOnTreatment = "Yes" THEN "Valid"
        ELSE "Invalid"
        END AS vl_valid
        FROM(
        SELECT *,
        DATE_DIFF(CURRENT_DATE("UTC"), vl_results_date, day) AS vl_days_since_test
        FROM `{PROJECT_ID}.{STAGING_DATASET}.merge_art_vls`) as days
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.valid_results',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    vl_suppression = BigQueryOperator(
        task_id='viral_load_suppression',
        sql=f'''
        SELECT *, CASE
        WHEN load_numbers < 1000 THEN "Suppressed"
        WHEN load_numbers >= 1000 THEN "Unsuppressed"
        WHEN load_numbers IS NULL THEN "Unknown"
        END AS viral_load_suppressed,
        FROM (
        SELECT *, CASE
        WHEN vl_test_result = "LDL" THEN 0
        WHEN vl_test_result != "LDL" THEN cast(vl_test_result as DECIMAL)
        END AS load_numbers,
        FROM `{PROJECT_ID}.{STAGING_DATASET}.valid_results`)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.viral_load_suppression',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    eligible = BigQueryOperator(
        task_id='eligible_for_VL',
        sql=f'''
        SELECT *, CASE
        WHEN vl_valid = "Unknown" THEN "Unknown"
        WHEN vl_valid = "Invalid" AND CurrentOnTreatment = "Yes" THEN "Eligible"
        WHEN vl_valid = "Valid" AND CurrentOnTreatment = "Yes" THEN "Test is current"
        ELSE "Ineligible"
        END AS vl_eligible
        FROM `{PROJECT_ID}.{STAGING_DATASET}.viral_load_suppression`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.eligible_for_VL',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    art_vls_warehouse = BigQueryOperator(
        task_id='art_vls_warehouse',
        sql=f'''
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.eligible_for_VL`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.art_mmd_vls',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    trigger = TriggerDagRunOperator(
        task_id ='trigger',
        trigger_dag_id='keemr_hts_transforms_play',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )

data_types >> deduplicate_VLS >> denullification_VLS >> viral_load >> latest_date >> single_records
single_records >> VLS_Warehouse >> art_vls >> valid_tests >> vl_suppression >> eligible
eligible >> art_vls_warehouse >> trigger