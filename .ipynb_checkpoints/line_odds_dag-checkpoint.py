# Imported libraries.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import hashlib
import time
import sqlalchemy as db
import numpy as np
import re
import json
import importlib

# my files
import etl

db_params = {
    "host": "mydatabase.cluster-codg2seac5vh.us-east-1.rds.amazonaws.com",
    "database": "mydatabase",  # Default database
    "user": "master_username",
    "password": "_zIR-.rkh0oEQ*mmcS?~WNHYnz01",
    "port": 5432
}

# default arguments for DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'line_odds_movements',  # DAG name
    default_args=default_args,
    description='This DAG scrapes the odds from sportsbet.com, formats the data into two tables (games, odds) and upload them to AWS RDS.',
    schedule_interval = timedelta(minutes=20), 
    catchup=False  # Prevent backfilling
)

# Define a task using PythonOperator
task = PythonOperator(
    task_id= 'get_odds',  # Task name
    python_callable=etl.main,  # Pass the function reference
    op_args=[db_params],  # Pass db_params as an argument to the function
    dag=dag,  # DAG to which this task belongs
    execution_timeout=timedelta(minutes=2),
)

# Run task
task
