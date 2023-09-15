# Importing modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.email import send_email
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 14),
    "email": ["paulodnobre@outlook.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

# Define the email subject and body
EMAIL_SUBJECT = "Airflow alert: DAG failed"
EMAIL_BODY = "Hi,\n\nThe DAG nyc_yellow_taxi_trips_to_postgresql has failed.\n\nRegards,\nAirflow"


def on_failure_callback(context):
    send_email(to=default_args["email"], subject=EMAIL_SUBJECT, html_content=EMAIL_BODY)


# Instantiate a DAG
dag = DAG(
    "nyc_yellow_taxi_trips_to_postgresql",
    default_args=default_args,
    description="Import NYC Yellow Taxi Trips data stored as parquet files into a PostgreSQL database",
    schedule_interval=timedelta(days=1),
    on_failure_callback=on_failure_callback,
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
