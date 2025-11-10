from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="ingest_csv_to_minio",
        bash_command="docker exec ingest python app.py"
    )

    spark_curate = BashOperator(
        task_id="spark_curate",
        bash_command="docker exec spark-master spark-submit /spark-jobs/job.py"
    )

    ingest >> spark_curate
