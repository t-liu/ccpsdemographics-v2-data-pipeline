import pytest
import tempfile
import os
from unittest.mock import Mock, patch
import pandas as pd

@pytest.fixture
def sample_csv_data():
    """Fixture providing sample CSV data"""
    return [
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
        }
    ]

@pytest.fixture
def temp_csv_file(sample_csv_data):
    """Fixture providing a temporary CSV file with test data"""
    df = pd.DataFrame(sample_csv_data)
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    df.to_csv(temp_file.name, index=False)
    temp_file.close()
    
    yield temp_file.name
    
    # Cleanup
    if os.path.exists(temp_file.name):
        os.unlink(temp_file.name)

@pytest.fixture
def mock_airflow_context():
    """Fixture providing mocked Airflow context"""
    context = {
        'ti': Mock(),
        'run_id': 'test_run_123',
        'dag_run': Mock(),
        'execution_date': Mock(),
        'ds': '2025-09-25'
    }
    context['dag_run'].dag_id = 'test_dag'
    return context

@pytest.fixture
def mock_mongodb_config():
    """Fixture providing mocked MongoDB configuration"""
    return {
        'connection_string': 'mongodb://testuser:testpass@localhost:27017/testdb',
        'database': 'testdb',
        'username': 'testuser',
        'password': 'testpass'
    }