.PHONY: test test-unit test-integration test-coverage clean

# Run all tests
test:
	pytest tests/ -v

# Run only unit tests
test-unit:
	pytest tests/ -m unit -v

# Run only integration tests
test-integration:
	pytest tests/ -m integration -v

# Run tests with coverage
test-coverage:
	pytest tests/ --cov=dags --cov-report=html --cov-report=term

# Run tests in parallel
test-parallel:
	pytest tests/ -n auto

# Clean test artifacts
clean:
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -name "*.pyc" -delete
	find . -name "__pycache__" -type d -exec rm -rf {} +
