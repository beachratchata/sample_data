from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import pandas as pd
import requests


my_database = 'mysql://beachratchata:*********@db4free.net:3306/beachdata?charset=utf8mb4'


def covidth_daily_into_db():

    url = 'https://covid19.th-stat.com/api/open/today'
    respon = requests.get(url)
    df_covid = pd.DataFrame(respon.json() , index=[0])
    df_covid['Date'] = pd.to_datetime(df_covid['UpdateDate']).dt.date
    df_covid['Date'] = pd.to_datetime(df_covid['Date'])
    df_covid = df_covid.drop(columns=['UpdateDate', 'Source','DevBy', 'SeverBy'])

    df_covid.to_sql('covidth' , my_database , if_exists='append' , index=False)


def get_covidth_data_from_db():

    qr = 'SELECT * FROM covidth;'
    covidth = pd.read_sql(qr , my_database)

    covidth.to_csv("/home/airflow/gcs/data/covidth.csv", index=False)


default_args = {
    'owner': 'beachratchata',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['beach1@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'schedule_interval': '@daily',
}

dag = DAG(
    'CovidTH', 
    default_args=default_args
)


t1 = DummyOperator(
    task_id='Start',
    dag=dag
)

t2 = PythonOperator(
    task_id='Covid_daily_into_db',
    python_callable=covidth_daily_into_db,
    dag=dag
)

t3 = PythonOperator(
    task_id='Covid_from_db',
    python_callable=get_covidth_data_from_db,
    dag=dag
)

t4 = EmailOperator(
    task_id='Send_Email',
    to = ['beach2@example.com'],
    subject= 'Done',
    html_content= 'get data from covid daily report , today is Done' , 
    dag=dag,

)

t5 = DummyOperator(
    task_id='End',
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5
