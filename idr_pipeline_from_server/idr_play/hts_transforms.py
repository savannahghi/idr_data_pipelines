import requests
from datetime import timedelta, datetime as dt
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow import models

GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID= models.Variable.get("PROJECT_ID")
STAGING_DATASET = models.Variable.get("HTS_play")
LOCATION = models.Variable.get("location")
WAREHOUSE = models.Variable.get("IDR_play")
MFL = models.Variable.get("mfl")

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

with DAG('keemr_hts_transforms_play', schedule_interval=None, default_args=default_args) as dag:

    data_types = BigQueryOperator(
        task_id='assign_appropriate_data_types',
        sql=f'''
        SELECT cast(PatientId as INT) as PatientId, cast(DOB as DATE) as DOB, cast(ageInYears as FLOAT64) as ageInYears, Gender, 
        FacilityName, SiteCode, CccNumber, cast(TestDate as DATE) as TestDate, EverTestedForHiv, cast(MonthsSinceLastTest as FLOAT64) as MonthsSinceLastTest, 
        ClientTestedAs, TestResult1, TestResult2, FinalTestResult, PatientGivenResult, TbScreening, ClientSelfTested, FacilityLinked, 
        cast(art_start_date as DATE) as art_start_date, CoupleDiscordant, TestType, EntryPoint, TestStrategy, Consent
        FROM `{PROJECT_ID}.{STAGING_DATASET}.staging`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.staging',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
    )

    deduplicate = BigQueryOperator(
        task_id='deduplicate_HTS',
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
        task_id='HTS_joining_MFL_Codes',
        sql=f'''
        SELECT MFL_Codes.SiteCode, MFL_Codes.county_name, MFL_Codes.sub_county_name, MFL_Codes.lat, MFL_Codes.long,
        MFL_codes.officialname as facility_name, Staging.CccNumber as ccc_number, Staging.PatientId, Staging.DOB, Staging.Gender, 
        Staging.ageInYears, Staging.EntryPoint as entrypoint, Staging.Consent as patient_consented, Staging.ClientTestedAs as client_tested_as, 
        Staging.TestStrategy as approach, Staging.TestResult1 as test_1_result, Staging.TestResult2 as test_2_result,
        Staging.FinalTestResult as final_test_result, Staging.TestDate as date_tested, Staging.PatientGivenResult as patient_given_result, 
        Staging.FacilityLinked as facility_linked_to, Staging.art_start_date, Staging.EverTestedForHiv as ever_tested_for_hiv, 
        Staging.MonthsSinceLastTest as months_since_last_test, Staging.TbScreening as tb_screening, 
        Staging.ClientSelfTested as client_self_tested, Staging.CoupleDiscordant as couple_discordant, Staging.TestType as test_type
        FROM `{PROJECT_ID}.{MFL}.MFL_Codes` as MFL_Codes
        INNER JOIN `{PROJECT_ID}.{STAGING_DATASET}.deduplicate` as Staging
        ON MFL_Codes.SiteCode =  cast(Staging.SiteCode as INT)
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.org_enrichment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    dates = BigQueryOperator(
        task_id='HTS_enriching_joined_table',
        sql=f'''
        SELECT *,
        DATE_DIFF(cast (art_start_date as date), cast (date_tested as date), day) AS LinkageDays,
        EXTRACT(YEAR FROM cast (date_tested as date)) AS date_tested_Year,
        EXTRACT(QUARTER FROM cast (date_tested as date)) AS date_tested_Quarter,
        EXTRACT(MONTH FROM cast (date_tested as date)) AS date_tested_Month,
        EXTRACT(YEAR FROM cast (art_start_date as date)) AS art_start_date_Year,
        EXTRACT(QUARTER FROM cast (art_start_date as date)) AS art_start_date_Quarter,
        EXTRACT(MONTH FROM cast (art_start_date as date)) AS art_start_date_Month,
        FROM `{PROJECT_ID}.{STAGING_DATASET}.org_enrichment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dates_enrichment',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_1 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint',
        sql=f'''
        SELECT *, CASE
        WHEN entrypoint = "CCC (comprehensive care center)" OR entrypoint = "CCC" THEN "CCC"
        WHEN entrypoint = "OPD (outpatient department)" OR entrypoint= "Out Patient Department(OPD)" THEN "OPD"
        WHEN entrypoint = "VCT center" OR entrypoint = "VCT" THEN "VCT"
        WHEN entrypoint = "Home based HIV testing program" THEN "Home Based Testing"
        WHEN entrypoint = "In Patient Department(IPD)" OR entrypoint= "INPATIENT CARE OR HOSPITALIZATION" THEN "IPD"
        WHEN entrypoint = "PMTCT ANC" OR entrypoint= "PMTCT MAT" OR entrypoint= "PMTCT Program" OR entrypoint= "PMTCT PNC" THEN "PMTCT"
        WHEN entrypoint = "OTHER NON-CODED" THEN "Other"
        WHEN entrypoint = "mobile VCT program" THEN "mobile VCT program"
        WHEN entrypoint = "Tuberculosis treatment program" THEN "Tuberculosis treatment program"
        WHEN entrypoint = "OB/GYN department" THEN "OB/GYN department"
        WHEN entrypoint is null THEN null
        ELSE entrypoint
        END AS entrypointclean
        FROM `{PROJECT_ID}.{STAGING_DATASET}.dates_enrichment`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_2 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint_2',
        sql=f'''
        SELECT *, CASE
        WHEN entrypoint = "CCC (comprehensive care center)" OR entrypoint = "CCC" THEN "0"
        WHEN entrypoint = "OPD (outpatient department)" OR entrypoint= "Out Patient Department(OPD)" THEN "0"
        WHEN entrypoint = "VCT center" OR entrypoint = "VCT" THEN "0"
        WHEN entrypoint = "Home based HIV testing program" THEN "0"
        WHEN entrypoint = "In Patient Department(IPD)" OR entrypoint= "INPATIENT CARE OR HOSPITALIZATION" THEN "0"
        WHEN entrypoint = "PMTCT ANC" OR entrypoint= "PMTCT MAT" OR entrypoint= "PMTCT Program" OR entrypoint= "PMTCT PNC" THEN "0"
        WHEN entrypoint = "OTHER NON-CODED" THEN "0"
        WHEN entrypoint = "mobile VCT program" THEN "0"
        WHEN entrypoint = "Tuberculosis treatment program" THEN "0"
        WHEN entrypoint = "OB/GYN department" THEN "0"
        WHEN entrypoint is null THEN null
        ELSE entrypoint
        END AS entrypointclean2
        FROM `{PROJECT_ID}.{STAGING_DATASET}.entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    entrypoint_3 = BigQueryOperator(
        task_id='HTS_enriching_entrypoint_3',
        sql=f'''
        SELECT *, CASE
        WHEN entrypointclean2 = "0" THEN entrypointclean
        WHEN entrypointclean2 is null THEN null
        ELSE "Other"
        END AS entrypointclean3
        FROM `{PROJECT_ID}.{STAGING_DATASET}.entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.entrypoints',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    warehouse_HTS = BigQueryOperator(
        task_id='HTS_data_warehouse',
        sql=f'''
        SELECT * FROM `{PROJECT_ID}.{STAGING_DATASET}.entrypoints`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.hts',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    summary_1 = BigQueryOperator(
        task_id='HTS_summary',
        sql=f'''
        SELECT * FROM (SELECT *, CASE
        WHEN LinkageDays = 0
        AND final_test_result = "Positive"
        THEN 'Same Day'
        WHEN LinkageDays > 0
        AND LinkageDays < 15
        AND final_test_result = "Positive" THEN ">1 day <2 weeks"
        WHEN LinkageDays >14
        AND final_test_result = "Positive" THEN ">2 weeks"
        WHEN LinkageDays < 0
        AND final_test_result = "Positive" THEN "Clerical Error"
        WHEN LinkageDays is null
        AND final_test_result = "Positive" THEN "Not Linked"
        END AS hts_cascade
        FROM `{PROJECT_ID}.{WAREHOUSE}.hts`)
        WHERE hts_cascade is not null
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{WAREHOUSE}.hts_summary',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    summary_2 = BigQueryOperator(
        task_id='HTS_warehouse_summary',
        sql=f'''
        SELECT
        SUM(CASE WHEN hts_cascade IS NOT NULL THEN 1 ELSE 0 END) AS totalPositive
        ,SUM(CASE WHEN hts_cascade ="Same Day" THEN 1 ELSE 0 END) AS sameDay
        ,SUM(CASE WHEN hts_cascade = ">1 day <2 weeks" THEN 1 ELSE 0 END) AS oneDayToTwoWeeks
        ,SUM(CASE WHEN hts_cascade = ">2 weeks" THEN 1 ELSE 0 END) AS moreThanTwoWeeks
        ,SUM(CASE WHEN hts_cascade = "Clerical Error" THEN 1 ELSE 0 END) AS clericalError
        ,SUM(CASE WHEN hts_cascade = "Not Linked" THEN 1 ELSE 0 END) AS notLinked
        FROM `{PROJECT_ID}.{WAREHOUSE}.hts_summary`
        ''',
        destination_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.hts_summary',
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        use_legacy_sql=False,
        bigquery_conn_id=GOOGLE_CONN_ID,
        dag=dag
        )

    trigger = TriggerDagRunOperator(
        task_id ='trigger',
        trigger_dag_id='keemr_covid_transforms_play',
        execution_date='{{ ds }}',
        reset_dag_run=True
    )


data_types >> deduplicate >> mfl >> dates >> entrypoint_1 >> entrypoint_2 >> entrypoint_3
entrypoint_3 >> warehouse_HTS >> summary_1 >> summary_2 >> trigger