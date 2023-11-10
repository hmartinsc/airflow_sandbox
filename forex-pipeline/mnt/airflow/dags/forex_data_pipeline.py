from datetime import datetime, timedelta
import csv
import requests
import json

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

DAG_ID = "forex_data_pipeline"

"""
default_args are the default arguments applied to all tasks in the DAG

owner:              is the owner of the DAG
email_on_failure:   is set to True to receive email alerts on failure
email_on_retry:     is set to False to not receive email alerts on retry
email:              is the email to receive alerts
retries:            is the number of retries that should be performed before failing the task
retry_delay:        is the time to wait between retries
"""
default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "example@axample.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

"""
Download forex rates according to the currencies we want to watch
described in the file forex_currencies.csv
"""
def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

"""
DAG is the core concept of Airflow. It is a collection of tasks that you want to run,
organized in a way that reflects their relationships and dependencies.

Arguments:

dag_id:             must be unique across all DAGs
default_args:       are the default arguments applied to all tasks in the DAG
tags:               are used to categorize DAGs
start_date:         is the date when the DAG will start running
schedule_interval:  is the frequency with which the DAG will be run
catchup:            is set to False to prevent backfilling of the DAG
"""
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    tags=["forex"],
    start_date=datetime(2023, 11, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    """
    is_forex_rates_available is a sensor that will wait for the API to be available

    Arguments:

    http_conn_id:   is the connection ID defined in the Airflow UI {conn_id="forex_api", conn_type="http", host="https://gist.github.com"}
    endpoint:       is the endpoint of the API
    response_check: is a function that will check the response of the API
    poke_interval:  is the time between each attempt to poke the API
    timeout:        is the time to wait before failing the task
    """
    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    """
    is_forex_currencies_file_available is a sensor that will wait for the file to be available

    Arguments:

    fs_conn_id:     is the connection ID defined in the Airflow UI {conn_id="forex_path", conn_type="file", extra={"path":"/opt/airflow/dags/files"}}
    filepath:       is the path of the file to check
    poke_interval:  is the time between each attempt to poke the API
    timeout:        is the time to wait before failing the task
    """
    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )

    """
    download_rates is a PythonOperator that will download the rates from the API

    Arguments:

    python_callable:    is the function to call
    """
    downloading_rates = PythonOperator(
        task_id="downloading_rates",
        python_callable=download_rates
    )

    """
    saving_rates is a BashOperator that will save the rates in HDFS

    Arguments:

    bash_command:   is the command to execute
    """
    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )

    """
    creating_forex_rates_table is a HiveOperator that will create the forex_rates table

    Arguments:

    hive_cli_conn_id:   is the connection ID defined in the Airflow UI {conn_id="hive_conn", conn_type="hive", host="hive", port=10000}
    hql:    is the Hive query to execute
    """
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates (
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )
    
    """
    forex_processing is a SparkSubmitOperator that will process the forex rates

    Arguments:

    application:    is the path to the application to submit
    conn_id:        is the connection ID defined in the Airflow UI {conn_id="spark_conn", conn_type="spark", host="spark://spark:7077", port=7077}
    verbose:        is set to False to not display logs
    """
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )
    
    """
    sending_email_notification is an EmailOperator that will send an email notification

    Arguments:

    to:             is the email to send the notification to
    subject:        is the subject of the email
    html_content:   is the content of the email
    """
    sending_email_notification = EmailOperator(
        task_id="sending_email_notification",
        to="hmartins19@gmail.com",	
        subject="Forex processing completed",
        html_content="<h3>Forex processing completed</h3>"
    )
    
    is_forex_rates_available >> is_forex_currencies_file_available
    is_forex_currencies_file_available >> downloading_rates
    downloading_rates >> saving_rates
    saving_rates >> creating_forex_rates_table
    creating_forex_rates_table >> forex_processing
    forex_processing >> sending_email_notification