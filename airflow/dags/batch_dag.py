from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="batch_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
):

    ingest = BashOperator(
        task_id="run_ingestion",
        bash_command="docker exec ingest python app/main.py"
    )

    transform = BashOperator(
        task_id="spark_transform",
        bash_command="docker exec spark spark-submit /spark_jobs/transform_job.py"
    )

    ingest >> transform
