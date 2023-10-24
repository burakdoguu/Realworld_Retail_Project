from datetime import timedelta
from airflow import DAG
from datetime import datetime
from airflow.decorators import dag

from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

from orders_kafka_consumer import fetch_and_insert_data_orders
from view_product_kafka_consumer import fetch_and_insert_data_views


@dag(

    schedule_interval="*/5 * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,    
)

def kafka_to_cassandra():

    start = EmptyOperator(task_id='start')

    consume_order_data = PythonOperator(
        task_id='consume_order_data',
        python_callable = fetch_and_insert_data_orders,
        execution_timeout=timedelta(seconds=70)
    )

    view_product_data = PythonOperator(
        task_id = 'view_product_data',
        python_callable = fetch_and_insert_data_views,
        execution_timeout=timedelta(seconds=45)
    )
    end = EmptyOperator(task_id='end')


    start >> [consume_order_data,view_product_data] >> end

kafka_to_cassandra()