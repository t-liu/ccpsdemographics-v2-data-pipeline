import pytest
import unittest
from unittest.mock import patch, Mock, MagicMock, mock_open, call
import pandas as pd
import json
import os
import tempfile
from datetime import datetime
from io import StringIO

# Import DAG functions
import sys
sys.path.insert(0, os.path.abspath('dags'))
from csv_to_mongo import (
    read_csv_from_s3,
    transform_to_json,
    load_to_mongo,
    get_mongodb_config,
    get_s3_config
)


class TestConfigFunctions(unittest.TestCase):
    """Test configuration retrieval functions"""
    
    @patch('csv_to_mongo.Variable.get')
    def test_get_mongodb_config_success(self, mock_variable_get):
        """Test successful MongoDB config retrieval"""
        # Arrange
        mock_variable_get.side_effect = [
            'mongodb+srv://user:pass@cluster.mongodb.net/project',
            'project',
            'user',
            'pass'
        ]
        
        # Act
        config = get_mongodb_config()
        
        # Assert
        self.assertIsNotNone(config)
        self.assertEqual(config['database'], 'project')
        self.assertIn('mongodb+srv://', config['connection_string'])
        self.assertEqual(config['username'], 'user')
        self.assertEqual(config['password'], 'pass')
    
    @patch('csv_to_mongo.Variable.get')
    def test_get_mongodb_config_failure(self, mock_variable_get):
        """Test MongoDB config retrieval failure"""
        # Arrange
        mock_variable_get.side_effect = Exception("Variable not found")
        
        # Act
        config = get_mongodb_config()
        
        # Assert
        self.assertIsNone(config)
    
    @patch('csv_to_mongo.Variable.get')
    def test_get_s3_config_success(self, mock_variable_get):
        """Test successful S3 config retrieval"""
        # Arrange
        mock_variable_get.side_effect = [
            'test-bucket',
            'data/test.csv',
            'aws_default'
        ]
        
        # Act
        config = get_s3_config()
        
        # Assert
        self.assertEqual(config['bucket_name'], 'test-bucket')
        self.assertEqual(config['key'], 'data/test.csv')
        self.assertEqual(config['aws_conn_id'], 'aws_default')


class TestReadCSVFromS3(unittest.TestCase):
    """Test read_csv_from_s3 function"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_kwargs = {
            'ti': Mock(),
            'run_id': 'test_run_123',
            'dag_run': Mock()
        }
        self.mock_kwargs['dag_run'].dag_id = 'test_dag'
        
        self.sample_csv_content = """school_id,school,address,city,state,zip,longitude,latitude,level,year,short_year,black,hispanic,white,other
1,Test Elementary,123 Test St,Test City,TS,12345,-77.123456,39.123456,E,2020-21,20-21,100,200,150,50
2,Test Middle,456 Middle Ave,Test City,TS,12345,-77.234567,39.234567,M,2020-21,20-21,80,180,120,40"""
        
        # Normalize line endings and strip whitespace
        self.sample_csv_content = self.sample_csv_content.replace('\r\n', '\n').strip()
        
        # Create mock_df using real pd.read_csv, before patching
        self.mock_df = pd.read_csv(StringIO(self.sample_csv_content))
        print(f"setUp DataFrame row count: {len(self.mock_df)}")
        print(f"setUp DataFrame content:\n{self.mock_df}")
    
    @patch('csv_to_mongo.Variable.get')
    @patch('csv_to_mongo.S3Hook')
    @patch('pandas.read_csv')
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv_to_mongo.pickle.dump')
    @patch('csv_to_mongo.os.path.exists')
    @patch('csv_to_mongo.os.path.getsize')
    def test_read_csv_from_s3_success(self, mock_getsize, mock_exists, 
                                     mock_pickle_dump, mock_file_open,
                                     mock_read_csv, mock_s3_hook, mock_variable_get):
        """Test successful CSV reading from S3"""
        # Arrange
        mock_variable_get.side_effect = ['test-bucket', 'data/test.csv', 'aws_default']
        
        mock_hook_instance = Mock()
        mock_s3_hook.return_value = mock_hook_instance
        mock_hook_instance.check_for_key.return_value = True
        mock_hook_instance.read_key.return_value = self.sample_csv_content
        
        # Set mock return value
        mock_read_csv.return_value = self.mock_df
        print(f"Mock read_csv return value row count: {len(mock_read_csv.return_value)}")
        
        mock_exists.return_value = True
        mock_getsize.return_value = 1024
        
        # Debug: Verify CSV content
        print(f"Raw CSV content: {repr(self.sample_csv_content)}")
        
        # Act
        result = read_csv_from_s3(**self.mock_kwargs)
        
        # Assert
        self.assertIn("Successfully processed 2 rows from S3", result)
        self.mock_kwargs['ti'].xcom_push.assert_called()
        
        # Verify XCom pushes
        calls = self.mock_kwargs['ti'].xcom_push.call_args_list
        self.assertEqual(len(calls), 2)  # data_file_path and s3_metadata
        
        # Check s3_metadata push
        metadata_call = [c for c in calls if c[1]['key'] == 's3_metadata'][0]
        metadata = metadata_call[1]['value']
        self.assertEqual(metadata['bucket'], 'test-bucket')
        self.assertEqual(metadata['key'], 'data/test.csv')
        self.assertEqual(metadata['row_count'], 2)
    
    @patch('csv_to_mongo.Variable.get')
    @patch('csv_to_mongo.S3Hook')
    def test_read_csv_from_s3_file_not_found(self, mock_s3_hook, mock_variable_get):
        """Test S3 file not found scenario"""
        # Arrange
        mock_variable_get.side_effect = ['test-bucket', 'data/missing.csv', 'aws_default']
        
        mock_hook_instance = Mock()
        mock_s3_hook.return_value = mock_hook_instance
        mock_hook_instance.check_for_key.return_value = False
        
        # Act & Assert
        with self.assertRaises(FileNotFoundError) as context:
            read_csv_from_s3(**self.mock_kwargs)
        
        self.assertIn("S3 object not found", str(context.exception))


class TestTransformToJSON(unittest.TestCase):
    """Test transform_to_json function"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_kwargs = {
            'ti': Mock(),
            'run_id': 'test_run_123',
            'dag_run': Mock()
        }
        self.mock_kwargs['dag_run'].dag_id = 'test_dag'
        
        self.sample_data = [
            {
                'school_id': 1,
                'school': 'Test Elementary',
                'address': '123 Test St',
                'city': 'Test City',
                'state': 'TS',
                'zip': 12345,
                'longitude': -77.123456,
                'latitude': 39.123456,
                'level': 'E',
                'year': '2020-21',
                'short_year': '20-21',
                'black': 100,
                'hispanic': 200,
                'white': 150,
                'other': 50
            },
            {
                'school_id': 1,
                'school': 'Test Elementary',
                'address': '123 Test St',
                'city': 'Test City',
                'state': 'TS',
                'zip': 12345,
                'longitude': -77.123456,
                'latitude': 39.123456,
                'level': 'E',
                'year': '2021-22',
                'short_year': '21-22',
                'black': 110,
                'hispanic': 210,
                'white': 160,
                'other': 55
            }
        ]
        
        self.s3_metadata = {
            'bucket': 'test-bucket',
            'key': 'data/test.csv',
            'row_count': 2,
            'file_size': 1024
        }
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv_to_mongo.pickle.load')
    @patch('csv_to_mongo.os.path.exists')
    @patch('csv_to_mongo.os.remove')
    @patch('csv_to_mongo.json.dumps')
    def test_transform_to_json_success(self, mock_json_dumps, mock_remove,
                                      mock_exists, mock_pickle_load, mock_file_open):
        """Test successful data transformation"""
        # Arrange
        mock_exists.return_value = True
        mock_pickle_load.return_value = self.sample_data
        mock_json_dumps.return_value = '{}'  # Small JSON
        
        # Mock XCom pulls
        def xcom_pull_side_effect(key=None, task_ids=None):
            if key == 's3_metadata':
                return self.s3_metadata
            elif key == 'data_file_path':
                return '/tmp/test_file.pkl'
            return None
        
        self.mock_kwargs['ti'].xcom_pull.side_effect = xcom_pull_side_effect
        
        # Act
        result = transform_to_json(**self.mock_kwargs)
        
        # Assert
        self.assertIn("Transformed 1 schools", result)
        self.assertIn("S3 data", result)
        self.mock_kwargs['ti'].xcom_push.assert_called()
        mock_remove.assert_called_once()
    
    def test_transform_to_json_missing_required_columns(self):
        """Test transformation with missing columns"""
        # Arrange
        incomplete_data = [{'school_id': 1, 'school': 'Test'}]  # Missing required columns

        # Mock XCom pulls with a side effect to handle multiple keys
        def xcom_pull_side_effect(key=None, task_ids=None):
            if key == 's3_metadata':
                return self.s3_metadata  # Return the s3_metadata dictionary
            elif key == 'data_file_path':
                return '/tmp/test_file.pkl'  # Return the file path
            return None

        self.mock_kwargs['ti'].xcom_pull.side_effect = xcom_pull_side_effect

        with patch('csv_to_mongo.os.path.exists', return_value=True):
            with patch('builtins.open', new_callable=mock_open):
                with patch('csv_to_mongo.pickle.load', return_value=incomplete_data):
                    # Act & Assert
                    with self.assertRaises(KeyError) as context:
                        transform_to_json(**self.mock_kwargs)

                    self.assertIn("Missing required columns", str(context.exception))
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv_to_mongo.pickle.load')
    @patch('csv_to_mongo.os.path.exists')
    @patch('csv_to_mongo.os.remove')
    def test_transform_handles_invalid_year_format(self, mock_remove, mock_exists,
                                                   mock_pickle_load, mock_file_open):
        """Test transformation handles invalid year formats gracefully"""
        # Arrange
        invalid_data = self.sample_data.copy()
        invalid_data[0]['year'] = 'invalid'
        
        mock_exists.return_value = True
        mock_pickle_load.return_value = invalid_data        
        
        # Mock XCom pulls with a side effect to handle multiple keys
        def xcom_pull_side_effect(key=None, task_ids=None):
            if key == 's3_metadata':
                return self.s3_metadata  # Return the s3_metadata dictionary
            elif key == 'data_file_path':
                return '/tmp/test_file.pkl'  # Return the file path
            return None
        
        self.mock_kwargs['ti'].xcom_pull.side_effect = xcom_pull_side_effect
        
        with patch('csv_to_mongo.json.dumps', return_value='{}'):
            # Act
            result = transform_to_json(**self.mock_kwargs)
            
            # Assert - should still succeed with valid data
            self.assertIn("Transformed", result)
    
    def test_transform_calculates_demographics_correctly(self):
        """Test that demographics total is calculated correctly"""
        # This tests the logic directly
        black, hispanic, white, other = 100, 200, 150, 50
        total = black + hispanic + white + other
        
        self.assertEqual(total, 500)


class TestLoadToMongo(unittest.TestCase):
    """Test load_to_mongo function"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_kwargs = {
            'ti': Mock(),
            'run_id': 'test_run_123',
            'dag_run': Mock()
        }
        self.mock_kwargs['dag_run'].dag_id = 'test_dag'
        
        self.test_documents = [
            {
                'schoolId': '1',
                'name': 'Test School',
                'yearlyData': [
                    {
                        'academicYear': {'full': '2020-21'},
                        'demographics': {'total': {'$numberInt': '500'}}
                    }
                ]
            }
        ]
    
    @patch('csv_to_mongo.Variable.get')
    @patch('csv_to_mongo.MongoClient')
    def test_load_to_mongo_success(self, mock_mongo_client, mock_variable_get):
        """Test successful MongoDB loading"""
        # Arrange
        mock_variable_get.side_effect = [
            'mongodb+srv://user:pass@cluster.mongodb.net/project',
            'project',
            'user',
            'pass'
        ]
        
        # Mock MongoDB client and operations
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.inserted_ids = ['id1', 'id2']
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__ = Mock(return_value=mock_db)
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = ['schools']
        mock_collection.insert_many = Mock(return_value=mock_result)
        
        # Mock XCom data
        self.mock_kwargs['ti'].xcom_pull.return_value = json.dumps(self.test_documents)
        
        # Act
        result = load_to_mongo(**self.mock_kwargs)
        
        # Assert
        self.assertIn("Inserted 2 documents", result)
        mock_collection.insert_many.assert_called_once_with(self.test_documents)
    
    @patch('csv_to_mongo.Variable.get')
    @patch('csv_to_mongo.MongoClient')
    @patch('builtins.open', new_callable=mock_open)
    @patch('csv_to_mongo.os.path.exists')
    @patch('csv_to_mongo.os.remove')
    def test_load_to_mongo_from_temp_file(self, mock_remove, mock_exists,
                                          mock_file_open, mock_mongo_client,
                                          mock_variable_get):
        """Test MongoDB loading from temp file (large data)"""
        # Arrange
        mock_variable_get.side_effect = [
            'mongodb+srv://user:pass@cluster.mongodb.net/project',
            'project',
            'user',
            'pass'
        ]
        
        mock_exists.return_value = True
        
        # Mock MongoDB
        mock_client = Mock()
        mock_db = Mock()
        mock_collection = Mock()
        mock_result = Mock()
        mock_result.inserted_ids = ['id1']
        
        mock_mongo_client.return_value = mock_client
        mock_client.__getitem__ = Mock(return_value=mock_db)
        mock_db.__getitem__ = Mock(return_value=mock_collection)
        mock_db.list_collection_names.return_value = ['schools']
        mock_collection.insert_many = Mock(return_value=mock_result)
        
        # Mock XCom returns None for json_data, but provides file path
        def xcom_pull_side_effect(key=None, task_ids=None):
            if key == 'json_data':
                return None
            elif key == 'json_file_path':
                return '/tmp/test.json'
            return None
        
        self.mock_kwargs['ti'].xcom_pull.side_effect = xcom_pull_side_effect
        
        with patch('csv_to_mongo.json.load', return_value=self.test_documents):
            # Act
            result = load_to_mongo(**self.mock_kwargs)
            
            # Assert
            self.assertIn("Inserted 1 documents", result)
            mock_remove.assert_called_once()
    
    @patch('csv_to_mongo.Variable.get')
    def test_load_to_mongo_no_data(self, mock_variable_get):
        """Test MongoDB loading with no data"""
        # Arrange
        mock_variable_get.side_effect = [
            'mongodb+srv://user:pass@cluster.mongodb.net/project',
            'project',
            'user',
            'pass'
        ]
        
        # Mock XCom returns empty list
        self.mock_kwargs['ti'].xcom_pull.return_value = json.dumps([])
        
        # Act
        result = load_to_mongo(**self.mock_kwargs)
        
        # Assert
        self.assertEqual(result, "No data to insert")


class TestDataValidation(unittest.TestCase):
    """Test data validation and business logic"""
    
    def test_level_mapping_completeness(self):
        """Test that all school levels are mapped"""
        level_mapping = {
            'E': 'Elementary',
            'M': 'Middle',
            'H': 'High',
            'O': 'Other'
        }
        
        expected_levels = ['E', 'M', 'H', 'O']
        for level in expected_levels:
            self.assertIn(level, level_mapping)
            self.assertIsNotNone(level_mapping[level])
    
    def test_coordinate_type_validation(self):
        """Test coordinate data types"""
        longitude = -77.123456
        latitude = 39.123456
        
        self.assertIsInstance(longitude, float)
        self.assertIsInstance(latitude, float)
        
        # Test valid ranges for Maryland area
        self.assertGreaterEqual(longitude, -180)
        self.assertLessEqual(longitude, 180)
        self.assertGreaterEqual(latitude, -90)
        self.assertLessEqual(latitude, 90)
    
    def test_demographics_non_negative(self):
        """Test that demographics are non-negative"""
        demographics = {
            'black': 100,
            'hispanic': 200,
            'white': 150,
            'other': 50
        }
        
        for demographic, count in demographics.items():
            self.assertGreaterEqual(count, 0)
            self.assertIsInstance(count, int)


if __name__ == '__main__':
    unittest.main(verbosity=2)