# airflow/dags/batch_dag.py (template)
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG('batch_processing_dag', start_date=datetime(2025,1,1), schedule_interval='@quarterly', catchup=False) as dag:
    ingest = BashOperator(task_id='ingest', bash_command='echo "ingest placeholder"')
    run_batch = BashOperator(task_id='run_batch', bash_command='echo "run batch placeholder"')
    ingest >> run_batch
