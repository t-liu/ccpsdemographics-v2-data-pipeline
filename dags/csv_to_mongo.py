
import os
import json
import time
import boto3
import pickle
import logging
import tempfile
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, ConfigurationError

# Default arguments for the DAG
default_args = {
    'owner': 't-liu',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 25),  # Temporary fallback
    'max_active_tasks': 16,
    'execution_timeout': timedelta(minutes=2),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'catchup': False
}

def get_mongodb_config():
    try:
        return {
            'connection_string': Variable.get("ccpsdemographics_conn_string"),
            'database': Variable.get("ccpsdemographics_database"),
            'username': Variable.get("ccpsdemographics_rw_user"),
            'password': Variable.get("ccpsdemographics_rw_password")
        }
    except Exception as e:
        print(f"Warning: Could not retrieve MongoDB config: {e}")
        return None

def get_s3_config():
    return {
        'bucket_name': Variable.get("s3_bucket_name"),
        'key': Variable.get("s3_csv_key"),
        'aws_conn_id': Variable.get("aws_conn_id")
    }

# Task 1: Read CSV
def read_csv_from_s3(**kwargs):   
    try:
        # Get S3 configuration
        s3_config = get_s3_config()
        bucket_name = s3_config['bucket_name']
        key = s3_config['key']
        aws_conn_id = s3_config['aws_conn_id']
        
        print(f"Reading CSV from S3: s3://{bucket_name}/{key}")
        
        # Method 1: Using S3Hook (recommended for Airflow)
        s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        
        # Check if the object exists
        if not s3_hook.check_for_key(key, bucket_name):
            raise FileNotFoundError(f"S3 object not found: s3://{bucket_name}/{key}")
        
        # Download the CSV content as string
        csv_content = s3_hook.read_key(key, bucket_name)
        print(f"Successfully downloaded {len(csv_content)} characters from S3")
        
        # Read CSV from string content
        df = pd.read_csv(StringIO(csv_content))
        print(f"Successfully parsed {len(df)} rows from CSV")
        print(f"DataFrame columns: {list(df.columns)}")
        
        # Convert to records
        records = df.to_dict(orient='records')
        
        # Create temp file for large data
        temp_dir = tempfile.gettempdir()
        run_id = kwargs.get('run_id', 'unknown')
        dag_id = kwargs.get('dag_run').dag_id if kwargs.get('dag_run') else 'unknown'
        temp_file = os.path.join(temp_dir, f"airflow_s3_{dag_id}_{run_id}.pkl")
        
        print(f"Saving data to temp file: {temp_file}")
        
        with open(temp_file, 'wb') as f:
            pickle.dump(records, f)
        
        # Verify file was created
        if os.path.exists(temp_file):
            file_size = os.path.getsize(temp_file)
            print(f"Temp file created successfully. Size: {file_size} bytes")
        else:
            raise FileNotFoundError(f"Failed to create temp file: {temp_file}")
        
        # Push file path and metadata to XCom
        kwargs['ti'].xcom_push(key='data_file_path', value=temp_file)
        kwargs['ti'].xcom_push(key='s3_metadata', value={
            'bucket': bucket_name,
            'key': key,
            'row_count': len(df),
            'file_size': len(csv_content)
        })
        
        print(f"Successfully processed {len(df)} rows from S3")
        return f"Successfully processed {len(df)} rows from S3"
        
    except Exception as e:
        print(f"Error in read_csv_from_s3: {str(e)}")
        raise

# Task 2: Transform to semi-structured JSON
def transform_to_json(**kwargs):
    """Transform CSV data to semi-structured JSON format for MongoDB"""
    ti = kwargs['ti']
    
    try:
        print("Starting transform_to_json task")
        
        # Get S3 metadata for logging
        s3_metadata = ti.xcom_pull(key='s3_metadata', task_ids='read_csv_from_s3')
        if s3_metadata:
            print(f"Processing data from S3: {s3_metadata['bucket']}/{s3_metadata['key']}")
            print(f"Original file size: {s3_metadata['file_size']} bytes, {s3_metadata['row_count']} rows")
        
        # Get file path from XCom
        file_path = ti.xcom_pull(key='data_file_path', task_ids='read_csv_from_s3')
        print(f"Retrieved file path from XCom: {file_path}")
        
        if file_path is None:
            all_xcoms = ti.xcom_pull(task_ids='read_csv_from_s3')
            print(f"All XComs from read_csv_from_s3 task: {all_xcoms}")
            raise ValueError("No file path found in XCom")
        
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Data file not found: {file_path}")
        
        # Load data from temp file
        print(f"Loading data from: {file_path}")
        with open(file_path, 'rb') as f:
            data = pickle.load(f)
        
        print(f"Loaded {len(data)} records from temp file")
        
        # Convert to DataFrame for processing
        df = pd.DataFrame(data)
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {list(df.columns)}")
        
        # Verify required columns exist
        required_columns = ['school_id', 'school', 'address', 'city', 'state', 'zip', 
                          'longitude', 'latitude', 'level', 'year', 'short_year',
                          'black', 'hispanic', 'white', 'other']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            raise KeyError(f"Missing required columns: {missing_columns}")

        # Group by school_id
        grouped = df.groupby('school_id')
        print(f"Found {len(grouped)} unique schools")
        
        # Map level to levelFull
        level_mapping = {
            'E': 'Elementary',
            'M': 'Middle',
            'H': 'High',
            'O': 'Other'
        }
        
        transformed_data = []
        current_timestamp = int(time.time() * 1000)
        
        for school_id, group in grouped:
            try:
                first_row = group.iloc[0]
                
                # Build yearlyData array
                yearly_data = []
                for _, row in group.iterrows():
                    year_parts = str(row['year']).split('-')
                    if len(year_parts) != 2:
                        continue
                        
                    try:
                        start_year = int(year_parts[0])
                        end_year = int(year_parts[1])
                    except ValueError:
                        continue
                    
                    try:
                        black = int(float(row['black']) if pd.notna(row['black']) else 0)
                        hispanic = int(float(row['hispanic']) if pd.notna(row['hispanic']) else 0)
                        white = int(float(row['white']) if pd.notna(row['white']) else 0)
                        other = int(float(row['other']) if pd.notna(row['other']) else 0)
                        total = black + hispanic + white + other
                    except (ValueError, TypeError):
                        continue
                    
                    yearly_data.append({
                        'academicYear': {
                            'full': str(row['year']),
                            'short': str(row['short_year']),
                            'startYear': {'$numberInt': str(start_year)},
                            'endYear': {'$numberInt': str(end_year)}
                        },
                        'demographics': {
                            'black': {'$numberInt': str(black)},
                            'hispanic': {'$numberInt': str(hispanic)},
                            'white': {'$numberInt': str(white)},
                            'other': {'$numberInt': str(other)},
                            'total': {'$numberInt': str(total)}
                        }
                    })
                
                if not yearly_data:
                    continue
                
                years = [int(yd['academicYear']['startYear']['$numberInt']) for yd in yearly_data]
                first_year = min(years)
                last_year = max(years)
                
                try:
                    longitude = float(first_row['longitude']) if pd.notna(first_row['longitude']) else 0.0
                    latitude = float(first_row['latitude']) if pd.notna(first_row['latitude']) else 0.0
                except (ValueError, TypeError):
                    longitude = 0.0
                    latitude = 0.0
                
                # Create school document with S3 source metadata
                school_doc = {
                    'schoolId': str(school_id),
                    'name': str(first_row['school']) if pd.notna(first_row['school']) else 'Unknown',
                    'location': {
                        'address': str(first_row['address']) if pd.notna(first_row['address']) else '',
                        'city': str(first_row['city']) if pd.notna(first_row['city']) else '',
                        'state': str(first_row['state']) if pd.notna(first_row['state']) else '',
                        'zipCode': str(int(float(first_row['zip']))) if pd.notna(first_row['zip']) else '',
                        'coordinates': {
                            'type': 'Point',
                            'coordinates': [
                                {'$numberDouble': str(longitude)},
                                {'$numberDouble': str(latitude)}
                            ]
                        }
                    },
                    'level': str(first_row['level']) if pd.notna(first_row['level']) else 'O',
                    'levelFull': level_mapping.get(str(first_row['level']), 'Unknown'),
                    'yearlyData': yearly_data,
                    'firstYear': {'$numberInt': str(first_year)},
                    'lastYear': {'$numberInt': str(last_year)},
                    'yearsOfData': {'$numberInt': str(len(years))},
                    'dataSource': {
                        'type': 'S3',
                        'bucket': s3_metadata['bucket'] if s3_metadata else 'unknown',
                        'key': s3_metadata['key'] if s3_metadata else 'unknown',
                        'processedAt': current_timestamp
                    },
                    'createdAt': {'$date': {'$numberLong': str(current_timestamp)}},
                    'updatedAt': {'$date': {'$numberLong': str(current_timestamp)}}
                }
                
                transformed_data.append(school_doc)
                
            except Exception as e:
                print(f"Error processing school {school_id}: {str(e)}")
                continue
        
        print(f"Successfully transformed data for {len(transformed_data)} schools")
        
        # Clean up temp file
        try:
            os.remove(file_path)
            print(f"Cleaned up temp file: {file_path}")
        except Exception as e:
            print(f"Warning: Could not clean up temp file: {e}")
        
        # Store transformed data
        if len(transformed_data) > 0:
            json_str = json.dumps(transformed_data)
            if len(json_str) > 40000:  # Use temp file for large data
                run_id = kwargs.get('run_id', 'unknown')
                dag_id = kwargs.get('dag_run').dag_id if kwargs.get('dag_run') else 'unknown'
                json_temp_file = os.path.join(tempfile.gettempdir(), f"airflow_json_{dag_id}_{run_id}.json")
                
                with open(json_temp_file, 'w') as f:
                    json.dump(transformed_data, f)
                
                ti.xcom_push(key='json_file_path', value=json_temp_file)
                print(f"JSON data saved to temp file: {json_temp_file}")
            else:
                ti.xcom_push(key='json_data', value=json_str)
                print("JSON data stored in XCom")
        
        return f"Transformed {len(transformed_data)} schools from S3 data"
        
    except Exception as e:
        print(f"Error in transform_to_json: {str(e)}")
        raise

# Task 3: Load to MongoDB
def load_to_mongo(**kwargs):
    """Load transformed data to MongoDB collection"""
    ti = kwargs['ti']
    
    try:
        print("Starting load_to_mongo task")
        
        # Try to get data from XCom first
        json_str = ti.xcom_pull(key='json_data', task_ids='transform_to_json')
        
        if json_str is None:
            # Try to get from temp file
            json_file_path = ti.xcom_pull(key='json_file_path', task_ids='transform_to_json')
            
            if json_file_path and os.path.exists(json_file_path):
                print(f"Loading data from temp file: {json_file_path}")
                with open(json_file_path, 'r') as f:
                    data = json.load(f)
                
                # Clean up temp file
                try:
                    os.remove(json_file_path)
                    print(f"Cleaned up JSON temp file: {json_file_path}")
                except Exception as e:
                    print(f"Warning: Could not clean up temp file: {e}")
            else:
                raise ValueError("No JSON data found in XCom or temp file")
        else:
            data = json.loads(json_str)
        
        print(f"Loaded {len(data)} documents for insertion")
        
        if not data:
            print("No data to insert")
            return "No data to insert"
        
        # Get MongoDB Atlas configuration
        mongo_config = get_mongodb_config()
        print("Retrieved MongoDB configuration")

        client = MongoClient(mongo_config['connection_string'])
            
        # Get the specific database and collection
        db = client[mongo_config['database']]
        collections_before = db.list_collection_names()
        print(f"📚 Collections in '{mongo_config['connection_string']}' before insert: {collections_before}")
            
        # Get the collection
        collection = db['schools']
        
        # Insert documents
        result = collection.insert_many(data)
        inserted_count = len(result.inserted_ids)
        
        print(f"Successfully inserted {inserted_count} documents into MongoDB")
        return f"Inserted {inserted_count} documents"
        
    except Exception as e:
        print(f"Error in load_to_mongo: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'ccpsdemographics_data_pipeline',
    default_args=default_args,
    description='ETL: CSV Flat File to MongoDB Collection',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'csv', 'mongodb', 'demographics']
)

# Define tasks
read_task = PythonOperator(
    task_id='read_csv_from_s3',
    python_callable=read_csv_from_s3,
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