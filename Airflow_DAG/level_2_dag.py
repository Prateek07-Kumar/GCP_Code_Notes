# Import All the Modules, Libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
# from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)


# Variable section
PROJECT_ID = "new-gcp-cloud-sql-project"
REGION = "europe-west1" 
CLUSTER_NAME = "airflowcluster"
JOB_FILE_URL = "gs://test-dag-source/main.py"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "secondary_worker_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {
            "boot_disk_type": "pd-standard",
            "boot_disk_size_gb": 32,
        },
        "is_preemptible": True,
        "preemptibility": "PREEMPTIBLE",
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URL},
}

args = {
    "owner": "shaik saidul",
    "start_date": datetime(2025, 10, 26), #(year, month day)
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# Define the DAG 
with DAG(
    "Level_2_DAG",
    schedule_interval="30 17 * * *",
    default_args=args,
    description='dataproc jobs'
) as DAG:
    
    
# Define all Task
    task_1 = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    
    task_2 = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    task_3 = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )
        

# Define the Dependencies
task_1 >> task_2 >> task_3
