from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from pyspark.sql.types import *
from datetime import datetime
from pathlib import Path
from dbt_airflow.core.task_group import DbtTaskGroup
from datetime import timedelta


spark_master = "spark://spark-master:7077"
#pack1 = "/opt/airflow/jars/spark-cassandra-connector_2.12-3.2.0.jar,/opt/airflow/jars/spark-core_2.12-3.2.3.jar,/opt/airflow/jars/spark-core_2.12-3.2.3.jar "
#pack2 = "/opt/airflow/jars/spark-core_2.12-3.2.3.jar "
#pack3 = "/opt/airflow/jars/spark-sql_2.12-3.2.3.jar"

#/opt/bitnami/spark/jars/spark-snowflake_2.12-2.10.0-spark_3.2.jar

with DAG(
    dag_id='cassandra_to_snowflake',
    schedule_interval='@daily',
    start_date=datetime(2022,1,1),
    tags=['test_snow'],
    catchup = False

) as dag:
    start = DummyOperator(task_id="start")

    spark_job_order = SparkSubmitOperator(
        task_id="spark_job_order",
        application= '/opt/airflow/dags/order_data_spark.py',
        conn_id="spark_conn",
        executor_cores=2,
        execution_timeout=timedelta(seconds=100),        
        #conf={"spark.master":spark_master},
        packages= "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.33"
        #jars=pack1, 
        #driver_class_path=pack2
    )

    spark_job_view = SparkSubmitOperator(
        task_id="spark_job_view",
        application= '/opt/airflow/dags/view_data_spark.py',
        conn_id="spark_conn",
        executor_cores=2,
        execution_timeout=timedelta(seconds=120),
        #conf={"spark.master":spark_master},
        packages= "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,net.snowflake:snowflake-jdbc:3.13.33"
        #jars=pack1, 
        #driver_class_path=pack2
    )


    retail_dbt = DbtTaskGroup(
        group_id='my-dbt-project',
        dbt_manifest_path=Path('/opt/airflow/dbt_retail/target/manifest.json'),
        dbt_target='dev',
        dbt_project_path=Path('/opt/airflow/dbt_retail/'),
        dbt_profile_path=Path('/opt/airflow/dbt_retail/'),
        create_sub_task_groups=False,
    )

    end = DummyOperator(task_id="end")

    start >> [spark_job_order,spark_job_view] >> retail_dbt >> end
  


#retail_dbt = BashOperator(
#    task_id="retail_dbt",
#    bash_command='dbt run',
#)
        