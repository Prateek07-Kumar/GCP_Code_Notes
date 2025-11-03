# Import All the Modules, Libraries
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


args = {
    "owner": "shaik saidul",
    "start_date": datetime(2025, 5, 24), #(year, month day)
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# Define the DAG 
with DAG(
    "PARENT_DAG_COMPOSER",
    schedule_interval = "30 17 * * *",
    default_args=args,
    # description='dataproc jobs'
) as dag:
    
    task_1 = TriggerDagRunOperator(
        task_id = "running_level1_dag",
        trigger_dag_id = "Level_1_DAG_TEST"
)
    
    task_2 = TriggerDagRunOperator(
        task_id = "running_level2_dag",
        trigger_dag_id = "Level_2_DAG"
)
    
# Dependency

(task_1,task_2)