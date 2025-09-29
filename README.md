## WIP: Data Engineering for CCPS Demographics

Work in progress.  This project is to illustrate a data pipeline developed using Python and orchestrated using Apache Airflow.  Source is a CSV file in an AWS S3 Bucket and target is a MongoDB Atlas collection.  

### Start locally in Python

```
python3 -m venv venv
. ./venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
export AIRFLOW_HOME=~/path/to/project
chmod -R 755 ~/path/to/project
```

Initialize the Airflow database, scheduler, and web server:
```
airflow db migrate
airflow scheduler & airflow api-server -p 8080
```

If DAGs need to be reimported into the web server:
```
airflow dags reserialize
airflow dags list
```

### Start locally in Python
To run test suite locally:
```
pip install -r requirements-test.txt
make test-coverage
```

### Technologies/Languages
* Application
    * Python
    * Airflow
    * MongoDB
* AWS
    * S3
* CI/CD
    * GitHub Actions