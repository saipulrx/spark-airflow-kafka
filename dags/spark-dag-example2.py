from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "ruangdata",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="spark_airflow_dag2",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="Spark Submit run 2 task",
    start_date=days_ago(1),
)

Extract = SparkSubmitOperator(
    application="/spark-scripts/spark-example.py",
    conn_id="spark_main",
    task_id="spark_submit_extract",
    dag=spark_dag,
)

aggregate_data = SparkSubmitOperator(
    application="/spark-scripts/sparksql-aggregate.py",
    conn_id="spark_main",
    task_id="spark_submit_aggregate",
    dag=spark_dag,
)

Extract >> aggregate_data
