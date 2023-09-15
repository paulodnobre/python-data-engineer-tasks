# Importing modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 14),
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}


# Instantiate a DAG
dag = DAG(
    "nyc_yellow_taxi_trips_to_postgresql",
    default_args=default_args,
    description="Import NYC Yellow Taxi Trips data stored as parquet files into a PostgreSQL database",
    schedule_interval=timedelta(days=1),
)

# Define the Airflow connections
spark_conn_id = "spark"

# Execute the Spark job to import the Parquet files into the PostgreSQL database
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id=spark_conn_id,
    application="/data/spark_job.py",
    packages="org.postgresql:postgresql:42.2.5",
    verbose=True,
    dag=dag,
)

# Define the end-of-DAG task
end_task = DummyOperator(task_id="end_task", dag=dag)

# Set the task dependencies
spark_job >> end_task
