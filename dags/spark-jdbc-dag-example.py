from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "ruangdata",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_jdbc_dag",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="run spark submit etl pipeline in airflow",
    start_date=days_ago(1),
)

etl = SparkSubmitOperator(
    application="/spark-scripts/spark-etl2.py",
    conn_id="spark_main",
    task_id="spark_submit_etl_task",
    dag=spark_dag,
)

etl
