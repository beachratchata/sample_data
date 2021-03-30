from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime , timedelta

from pandas_datareader import data
from sqlalchemy import create_engine
import pandas as pd 
import pymysql.cursors


db_data = 'mysql+mysqldb://'+'ratchata'+':'+'123456789'+'@'+'db4free.net'+':3306/'+'usatest'+'?charset=utf8mb4'
engine = create_engine(db_data)


def get_daily_thbk() :

    lis_name = ['bay.bk','bbl.bk','cimbt.bk','kbank.bk','kkp.bk','ktb.bk','scb.bk','tmb.bk']
    today = datetime.today().strftime('%Y-%m-%d')
    daily_thbk = []

    try :
        dfs = data.DataReader(lis_name , 'yahoo' , today  , today)
        dfs = dfs.stack().reset_index()
        daily_thbk.append(dfs)

    except :
        pass

    if not daily_thbk :
        print('Emty')
    else :
        daily_thbk[0].to_sql('demoset', engine, if_exists='append' , index=False)


def get_data_from_db() :

    connection = pymysql.connect(host='db4free.net',
                                port=3306,
                                user='ratchata',
                                password='123456789',
                                db='usatest',
                                charset='utf8mb4',
                                cursorclass=pymysql.cursors.DictCursor)

    with connection.cursor() as cursor:
        sql = "SELECT * from demoset"
        cursor.execute(sql)
        thbk = cursor.fetchall()

    thaibank = pd.DataFrame(thbk)
    thaibank.to_csv("/home/airflow/gcs/data/thaibank.csv", index=False)

default_args = {
    'owner': 'ratchata',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily', }

dag = DAG(
    'ratchata',
    default_args=default_args,
    description='ratchata Data Pipeline ',
)

t1 = DummyOperator(
    task_id='Start',
    dag=dag
)

t2 = PythonOperator(
    task_id='get_daily',
    python_callable=get_daily_thbk,
    dag=dag
)

t3 = PythonOperator(
    task_id='get_data_from_db',
    python_callable=get_data_from_db,
    dag=dag
)

t4 = DummyOperator(
    task_id='End',
    dag=dag
)

t1 >> t2 >> t3 >> t4


