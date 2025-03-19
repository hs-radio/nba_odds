# Imported libraries.
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as db
import numpy as np

# Function to query the database
def main():
    print('hello world')
    
    # Database parameters
    db_params = {
        "host": "mydatabase.cluster-codg2seac5vh.us-east-1.rds.amazonaws.com",
        "database": "mydatabase",
        "user": "master_username",
        "password": "IW08Q5b*uMJd|Hxnl7lnXbE-p!!H",
        "port": 5432
    }


    # Create a one-row DataFrame
    data = {
        'id': [np.random.randint(10**6)],
        'first_name': ['John'],
        'last_name': ['Doe'],
        'age': [30],
        'department': ['Engineering']
    }
    print('----------------------------')
    df = pd.DataFrame(data)
    print(df)
    
    # Create the PostgreSQL connection string
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = db.create_engine(db_url)

    # connect to AWS
    with engine.connect() as conn:

        # Upload the DataFrame to the 'employees' table
        print('----- B -----------------')
        df.to_sql('employees', engine, if_exists='append', index=False)
        print(" !!!!!!!!!!   Data uploaded successfully !!!!!!!!!!!!!!!")
    
        # Pull the 'employees' table from the database into a DataFrame
        query = "SELECT * FROM employees"
        employees_df = pd.read_sql(query, engine)
        
        # Print the DataFrame
        print(employees_df)



# Default arguments for DAG
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
    'test',  # DAG name
    default_args=default_args,
    description='Just a test',
    schedule_interval=timedelta(minutes=10), 
    catchup=False  # Prevent backfilling
)

# Define a task using PythonOperator
task = PythonOperator(
    task_id='test_task',  # Task name
    python_callable=main,  # Correct function to be called
    dag=dag,  # DAG to which this task belongs
)

# Run task
task
