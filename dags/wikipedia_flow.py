import os
import sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import (
    extract_wikipedia_data,
    transform_wikidata_data,
    write_wikipedia_data
)

with DAG(
    dag_id='wikipedia_flow',
    default_args={"owner": "Rahul", "start_date": datetime(2025, 6, 20)},
    schedule_interval=None,
    catchup=False
) as dag:

    extract_data_from_wiki = PythonOperator(
        task_id='extract_data_from_wiki',
        python_callable=extract_wikipedia_data,
        provide_context=True,
        op_kwargs={
            "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
        }
    )

    transform_data_from_wiki = PythonOperator(
        task_id='transform_data_from_wiki',
        python_callable=transform_wikidata_data,
        provide_context=True,
        op_kwargs={
            "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
        }
    )

    write_data_to_file = PythonOperator(
        task_id='write_wikipedia_data',
        python_callable=write_wikipedia_data,
        provide_context=True
    )

extract_data_from_wiki >> transform_data_from_wiki >> write_data_to_file
