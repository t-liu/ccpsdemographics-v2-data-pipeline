from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from pymongo import MongoClient
import json

# Default arguments for the DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2025, 9, 24),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'csv_to_mongo_pipeline',
    default_args=default_args,
    description='ETL: CSV Flat File to MongoDB Collection',
    schedule='@daily',
    catchup=False,
)

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
    
    # Example transformation: Assume CSV has columns like 'id', 'name', 'address_street', 'address_city'
    # Nest 'address' into a sub-dict for semi-structured format
    transformed_data = []
    for row in data:
        transformed_row = {
            'id': row.get('id'),
            'name': row.get('name'),
            'address': {
                'street': row.get('address_street'),
                'city': row.get('address_city'),
                # Add more nesting as needed
            },
            # Add any other fields or logic here (e.g., data cleaning, aggregations)
        }
        transformed_data.append(transformed_row)
    
    # Convert to JSON string for easier storage/transfer
    json_data = json.dumps(transformed_data)
    ti.xcom_push(key='json_data', value=json_data)
    print("Data transformed to semi-structured JSON.")

# Task 3: Load to MongoDB
def load_to_mongo(**kwargs):
    ti = kwargs['ti']
    json_str = ti.xcom_pull(key='json_data')
    data = json.loads(json_str)
    
    # MongoDB connection (replace with your details)
    mongo_uri = 'mongodb://localhost:27017/'  # Or 'mongodb+srv://user:pass@cluster.mongodb.net/'
    client = MongoClient(mongo_uri)
    db = client['your_database_name']  # Replace with your DB name
    collection = db['your_collection_name']  # Replace with your collection name
    
    # Insert data (use insert_many for bulk)
    if data:
        collection.insert_many(data)
        print(f"Inserted {len(data)} documents into MongoDB.")
    else:
        print("No data to insert.")
    
    client.close()

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