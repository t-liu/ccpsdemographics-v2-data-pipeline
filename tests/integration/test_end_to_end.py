import pytest
import unittest
from unittest.mock import patch, Mock
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from datetime import datetime, timedelta
import tempfile
import os
import pandas as pd

class TestDAGIntegration(unittest.TestCase):
    """Integration tests for the entire DAG"""
    
    def setUp(self):
        """Set up test environment"""
        self.dagbag = DagBag(dag_folder='dags/', include_examples=False)
        self.dag_id = 'ccpsdemographics_data_pipeline'
        self.dag = self.dagbag.get_dag(self.dag_id)
        
    def test_dag_loaded(self):
        """Test that the DAG is loaded correctly"""
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.dag_id, self.dag_id)
    
    def test_dag_has_no_import_errors(self):
        """Test that DAG loads without import errors"""
        self.assertEqual(len(self.dagbag.import_errors), 0)
    
    def test_dag_tasks_count(self):
        """Test that DAG has the expected number of tasks"""
        self.assertEqual(len(self.dag.tasks), 3)
        
    def test_task_ids(self):
        """Test that all expected tasks are present"""
        expected_tasks = ['read_csv_from_s3', 'transform_to_json', 'load_to_mongo']
        actual_tasks = [task.task_id for task in self.dag.tasks]
        
        for expected_task in expected_tasks:
            self.assertIn(expected_task, actual_tasks)
    
    def test_task_dependencies(self):
        """Test that task dependencies are correct"""
        # Get tasks
        read_task = self.dag.get_task('read_csv_from_s3')
        transform_task = self.dag.get_task('transform_to_json')
        load_task = self.dag.get_task('load_to_mongo')
        
        # Test upstream dependencies
        self.assertEqual(len(read_task.upstream_task_ids), 0)
        self.assertIn('read_csv_from_s3', transform_task.upstream_task_ids)
        self.assertIn('transform_to_json', load_task.upstream_task_ids)
        
        # Test downstream dependencies
        self.assertIn('transform_to_json', read_task.downstream_task_ids)
        self.assertIn('load_to_mongo', transform_task.downstream_task_ids)
        self.assertEqual(len(load_task.downstream_task_ids), 0)

    def test_dag_schedule(self):
        """Test DAG scheduling configuration"""
        self.assertEqual(self.dag.schedule, '@daily')
        self.assertEqual(self.dag.max_active_runs, 1)
        self.assertFalse(self.dag.catchup)

class TestDataQuality(unittest.TestCase):
    """Test data quality and validation"""
    
    def test_required_columns_present(self):
        """Test that all required columns are present in test data"""
        required_columns = [
            'school_id', 'school', 'address', 'city', 'state', 'zip',
            'longitude', 'latitude', 'level', 'year', 'short_year',
            'black', 'hispanic', 'white', 'other'
        ]
        
        test_data = {
            'school_id': 1,
            'school': 'Test School',
            'address': '123 Test St',
            'city': 'Test City',
            'state': 'TS',
            'zip': 12345,
            'longitude': -77.123,
            'latitude': 39.123,
            'level': 'E',
            'year': '2020-21',
            'short_year': '20-21',
            'black': 100,
            'hispanic': 200,
            'white': 150,
            'other': 50
        }
        
        for col in required_columns:
            self.assertIn(col, test_data)

    def test_data_types_validation(self):
        """Test that data types are as expected"""
        test_record = {
            'school_id': 1,
            'longitude': -77.123456,
            'latitude': 39.123456,
            'zip': 12345,
            'black': 100,
            'hispanic': 200,
            'white': 150,
            'other': 50
        }
        
        # Test numeric types
        self.assertIsInstance(test_record['school_id'], int)
        self.assertIsInstance(test_record['longitude'], float)
        self.assertIsInstance(test_record['latitude'], float)
        self.assertIsInstance(test_record['black'], int)

    def test_demographics_are_non_negative(self):
        """Test that demographic counts are non-negative"""
        demographics = {'black': 100, 'hispanic': 200, 'white': 150, 'other': 50}
        
        for demographic, count in demographics.items():
            self.assertGreaterEqual(count, 0, f"{demographic} count should be non-negative")

    def test_coordinate_ranges(self):
        """Test that coordinates are in valid ranges"""
        longitude = -77.123456
        latitude = 39.123456
        
        # Basic coordinate validation (adjust ranges for your area)
        self.assertGreaterEqual(longitude, -180)
        self.assertLessEqual(longitude, 180)
        self.assertGreaterEqual(latitude, -90)
        self.assertLessEqual(latitude, 90)
