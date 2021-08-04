from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.postgres_operator import PostgresOperator

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
postgres_driver_jar = "/usr/local/spark/resources/jars/postgresql-9.4.1207.jar"

trips_file="/usr/local/spark/app/trips.csv"
postgres_db = "jdbc:postgresql://postgres/test"
postgres_user = "test"
postgres_pwd = "postgres"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-postgres", 
        description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
        default_args=default_args, 
        schedule_interval="00 1 * * *"
    )

start 	= DummyOperator(task_id='start',start_date=datetime(now.year, now.month, now.day), retries=3)

spark_job_load_postgres = SparkSubmitOperator(
    task_id="spark_job_load_postgres",
    application="/usr/local/spark/app/load-postgres.py", # Spark application path created in airflow and spark cluster
    name="load-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[trips_file,postgres_db,postgres_user,postgres_pwd],
    jars=postgres_driver_jar,
    driver_class_path=postgres_driver_jar,
    dag=dag)

create_datawarehouse_tables = PostgresOperator(
    task_id="create_datawarehouse_tables",
    postgres_conn_id="postgres_default",
    sql="./sql/create_datawarehouse_tables.sql",
    start_date=datetime(now.year, now.month, now.day),
    dag=dag)

update_datawarehouse_tables = PostgresOperator(
    task_id="update_datawarehouse_tables",
    postgres_conn_id="postgres_default",
    sql="./sql/update_datawarehouse_tables.sql",
    start_date=datetime(now.year, now.month, now.day),
    dag=dag)

#   email = EmailOperator(
#         task_id='send_email',
#         to='eduardo172a@gmail.com',
#         subject='Airflow Alert',
#         html_content=""" <h3>Extraction and transformation complete</h3> """)
email = DummyOperator(
    task_id='send_email',
    start_date=datetime(now.year, now.month, now.day),
    dag=dag)

end = DummyOperator(task_id="end",start_date=datetime(now.year, now.month, now.day), dag=dag)

start >> spark_job_load_postgres >> create_datawarehouse_tables >> update_datawarehouse_tables >> email  >> end