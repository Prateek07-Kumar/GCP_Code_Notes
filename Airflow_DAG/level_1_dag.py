# Import All the Modules, Libraries

import airflow
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Variable Section

PROJECT_ID = "new-gcp-cloud-sql-project"
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "emp_raw"
TABLE_NAME_2 = "dep_raw"
TABLE_NAME_3 = "empDep_in"
LOCATION = "US"

INSERT_ROWS_QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
    e.EmployeeID,
    CONCAT(e.FirstName, " ", e.LastName) AS FullName,
    e.Email,
    e.Salary,
    e.JoinDate,
    d.DepartmentID,
    d.DepartmentName,
    CAST(e.Salary AS INT64) * 0.01 AS Tax
FROM
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN
    `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON
    e.DepartmentID = d.DepartmentID;
"""

args = {
    "owner": "shaik saidul",
    "start_date": datetime(2025, 10, 23), #(YEAR, MONTH, DAY)
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

# Define the DAG 
with DAG(
    "Level_1_DAG_TEST",
    schedule_interval="30 17 * * *",
    default_args=args,
    description='gcs to bigquery job'
) as DAG:

    # Define the Tasks

    task_1 = GCSToBigQueryOperator(
        task_id="employee_example",
        bucket="test-dag-source",
        source_objects=["employees.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Salary", "type": "STRING", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_2 = GCSToBigQueryOperator(
        task_id="department_example",  # Changed task_id to make it unique
        bucket="test-dag-source",
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    task_3 = BigQueryInsertJobOperator(
        task_id="empDep_table",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )

    # Define the Dependency
    (task_1, task_2) >> task_3
