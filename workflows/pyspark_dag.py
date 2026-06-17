# import all modules
import os
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)

# define the variables
PROJECT_ID = os.environ.get("PROJECT_ID", "dcn-development")
REGION = os.environ.get("REGION", "asia-south1")
CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER_NAME", "my-demo-cluster1")
COMPOSER_BUCKET = os.environ.get("COMPOSER_BUCKET", "asia-south1-dcn-healthcare--e3f496b8-bucket")

# Environment variables to pass to PySpark jobs
MYSQL_USER = os.environ.get("MYSQL_USER", "myuser")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "Welcome!1234")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "dcn-healthcare-bucket")

JOB_PROPERTIES = {
    "spark.executorEnv.MYSQL_USER": MYSQL_USER,
    "spark.executorEnv.MYSQL_PASSWORD": MYSQL_PASSWORD,
    "spark.executorEnv.GCS_BUCKET": GCS_BUCKET,
    "spark.yarn.appMasterEnv.MYSQL_USER": MYSQL_USER,
    "spark.yarn.appMasterEnv.MYSQL_PASSWORD": MYSQL_PASSWORD,
    "spark.yarn.appMasterEnv.GCS_BUCKET": GCS_BUCKET,
}

GCS_JOB_FILE_1 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "properties": JOB_PROPERTIES
    },
}

GCS_JOB_FILE_2 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_2,
        "properties": JOB_PROPERTIES
    },
}

GCS_JOB_FILE_3 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_3,
        "properties": JOB_PROPERTIES
    },
}

GCS_JOB_FILE_4 = f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_4,
        "properties": JOB_PROPERTIES
    },
}


ARGS = {
    "owner": "Saurabh Sinha",
    "start_date": None,
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["***@gmail.com"],
    "email_on_success": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# define the dag
with DAG(
    dag_id="pyspark_dag",
    schedule_interval=None,
    description="DAG to start a Dataproc cluster, run PySpark jobs, and stop the cluster",
    default_args=ARGS,
    tags=["pyspark", "dataproc", "etl", "marvel"]
) as dag:
    
    # define the Tasks
    start_cluster = DataprocStartClusterOperator(
        task_id="start_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id="pyspark_task_1", 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task_2", 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id="pyspark_task_3", 
        job=PYSPARK_JOB_3, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id="pyspark_task_4", 
        job=PYSPARK_JOB_4, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

# define the task dependencies
start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster