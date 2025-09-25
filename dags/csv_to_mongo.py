import json
import time
import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable

# Default arguments for the DAG
default_args = {
    'owner': 't-liu',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'catchup': False
}

def get_mongodb_config():
    return {
        'connection_string': Variable.get("ccpsdemographics_read_conn_string"),
        'database': Variable.get("ccpsdemographics_read_database"),
        'username': Variable.get("ccpsdemographics_read_user"),
        'password': Variable.get("ccpsdemographics_read_password")
    }

# Task 1: Read CSV
def read_csv(**kwargs):
    csv_path = '/data/ccps_data.csv'
    df = pd.read_csv(csv_path)
    kwargs['ti'].xcom_push(key='csv_data', value=df.to_dict(orient='records'))
    print(f"Read {len(df)} rows from CSV.")

# Task 2: Transform to semi-structured JSON
def transform_to_json(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='csv_data')
    
    # Group by school_id
    df = pd.DataFrame(data)
    grouped = df.groupby('school_id')
    
    # Map level to levelFull
    level_mapping = {
        'E': 'Elementary',
        'M': 'Middle',
        'H': 'High',
        'O': 'Other'
    }
    
    transformed_data = []
    current_timestamp = int(time.time() * 1000)  # Current time in milliseconds
    
    for school_id, group in grouped:
        # Extract static school attributes (assume consistent across rows)
        first_row = group.iloc[0]
        
        # Build yearlyData array
        yearly_data = []
        for _, row in group.iterrows():
            start_year = int(row['year'].split('-')[0])
            end_year = int(row['year'].split('-')[1])
            total = int(row['black']) + int(row['hispanic']) + int(row['white']) + int(row['other'])
            yearly_data.append({
                'academicYear': {
                    'full': row['year'],
                    'short': row['short_year'],
                    'startYear': {'$numberInt': str(start_year)},
                    'endYear': {'$numberInt': str(end_year)}
                },
                'demographics': {
                    'black': {'$numberInt': str(int(row['black']))},
                    'hispanic': {'$numberInt': str(int(row['hispanic']))},
                    'white': {'$numberInt': str(int(row['white']))},
                    'other': {'$numberInt': str(int(row['other']))},
                    'total': {'$numberInt': str(total)}
                }
            })
        
        # Compute firstYear, lastYear, yearsOfData
        years = [int(row['year'].split('-')[0]) for _, row in group.iterrows()]
        first_year = min(years)
        last_year = max(years)
        
        # Create school document
        school_doc = {
            'schoolId': str(school_id),
            'name': first_row['school'],
            'location': {
                'address': first_row['address'],
                'city': first_row['city'],
                'state': first_row['state'],
                'zipCode': str(first_row['zip']),
                'coordinates': {
                    'type': 'Point',
                    'coordinates': [
                        {'$numberDouble': str(float(first_row['longitude']))},
                        {'$numberDouble': str(float(first_row['latitude']))}
                    ]
                }
            },
            'level': first_row['level'],
            'levelFull': level_mapping.get(first_row['level'], 'Unknown'),
            'yearlyData': yearly_data,
            'firstYear': {'$numberInt': str(first_year)},
            'lastYear': {'$numberInt': str(last_year)},
            'yearsOfData': {'$numberInt': str(len(years))},
            'createdAt': {'$date': {'$numberLong': str(current_timestamp)}},
            'updatedAt': {'$date': {'$numberLong': str(current_timestamp)}}
        }
        
        transformed_data.append(school_doc)
    
    json_data = json.dumps(transformed_data)
    ti.xcom_push(key='json_data', value=json_data)
    print(f"Transformed data for {len(transformed_data)} schools.")

# Task 3: Load to MongoDB
def load_to_mongo(**kwargs):
    ti = kwargs['ti']
    json_str = ti.xcom_pull(key='json_data')
    data = json.loads(json_str)
    
    # Get MongoDB Atlas configuration
    mongo_config = get_mongodb_config()
    
    # Initialize MongoHook with connection string
    hook = MongoHook(
        conn_id='mongo_conn',
        mongo_conn_str=mongo_config['connection_string'],
        mongo_db=mongo_config['database']
    )
    
    collection = hook.get_collection('schools', mongo_config['database'])
    if data:
        collection.insert_many(data)
        print(f"Inserted {len(data)} documents into MongoDB.")

# Define the DAG
dag = DAG(
    'ccpsdemographics_data_pipeline',
    default_args=default_args,
    description='ETL: CSV Flat File to MongoDB Collection',
    schedule='@daily',
    catchup=False,
)

# Define tasks
read_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_to_json',
    python_callable=transform_to_json,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_mongo',
    python_callable=load_to_mongo,
    dag=dag,
)

# Set task dependencies
read_task >> transform_task >> load_task