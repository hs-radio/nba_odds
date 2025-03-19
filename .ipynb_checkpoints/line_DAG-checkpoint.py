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
import ETL as etl

def main():
    # URL to scrape
    sportsbet_url = "https://www.sportsbet.com.au/betting/basketball-us/nba"
    print_results = False
    
    # Initialize an empty games and odds table
    games = pd.DataFrame(columns=["GameID", "AwayTeam", "HomeTeam", "StartDate", "StartTime"])
    odds = pd.DataFrame(columns=["GameID", "Date", "Time", "Live","AwayOdds", "HomeOdds"])
    
    # get event containers from sportsbet website where all the data is stored.
    event_containers = etl.get_event_containers_sportsbet(sportsbet_url)
    
    # Loop through each event container
    for event in event_containers:
        # get data
        team_one_name, team_two_name, live, odds_text, dt = etl.extract_match_properties(event, print_results)
        
        # Generate a unique GameID
        gameID = etl.generate_game_id(team_one_name, team_two_name, dt.date())
    
        # put the data into tables.
        games, odds = etl.update_tables(games, odds, gameID, team_one_name, team_two_name, live, odds_text, dt)

    # Load data to AWS
    etl.upload_data_to_AWS(games, odds, db_params)
    print(f"{len(odds)} odds added to database.")

db_params = {
    "host": "mydatabase.cluster-codg2seac5vh.us-east-1.rds.amazonaws.com",
    "database": "mydatabase",  # Default database
    "user": "master_username",
    "password": "IW08Q5b*uMJd|Hxnl7lnXbE-p!!H",
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
    python_callable = main,  # Correct function to be called
    dag=dag,  # DAG to which this task belongs
    execution_timeout=timedelta(minutes=2),
)

# Run task
task
