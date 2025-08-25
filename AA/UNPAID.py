from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
from pandas import NaT
from datetime import datetime, timedelta
import dateutil.relativedelta as rt
import pymssql
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tqdm.notebook import tqdm
import time
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import pyodbc
import snowflake.connector
import pandas as pd
import pymssql
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from snowflake import connector
from airflow.hooks.dbapi import DbApiHook
import sqlalchemy as sa
from sqlalchemy import create_engine
from typing import Any, Dict, Optional, Tuple, Union
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect as snowflake_connector
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import csv
import pytz
import pendulum





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'RECIVABLE_MART',
    default_args=default_args,
    description='ETL from MSSQL to Snowflake',
    start_date=pendulum.datetime(2024, 12, 12, tz="Asia/Seoul"),
    schedule_interval='0 1 * * 1-7',  
    catchup=False,
)

class MsSqlHook(DbApiHook):
    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    conn_type = 'mssql'
    hook_name = 'Microsoft SQL Server'
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self) -> pymssql.connect:
        conn = self.get_connection(self.mssql_conn_id)
        return pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port,
            charset='cp949'
        )

    def set_autocommit(self, conn: pymssql.connect, autocommit: bool) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, conn: pymssql.connect):
        return conn.autocommit_state


# MSSQL 커넥터 함수
def MSSQL_CONNECTOR():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor

# Snowflake 커넥터 함수
def SNOWFLAKE_CONNECTOR():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('USE ROLE ACCOUNTADMIN')
    return conn, cursor

# AGG_TABLE 함수 정의
def AGG_TABLE():
    from_conn, from_cursor = MSSQL_CONNECTOR()
    to_conn, to_cursor = SNOWFLAKE_CONNECTOR()
    overall_row_count = 0
    start = datetime.now()
    today = datetime.today()
    for month_tick in range(-1, 24):
        END_DATE = (today.replace(day=1) - relativedelta(months=month_tick, days=1)).strftime('%Y-%m-%d')

        ISM_query = f"""EXEC [EC00701_SELECT_COLL_GROUP_DC_ADD] 1,-1,-1,-1,'{END_DATE}', 'Z', 'N', ''"""

        from_cursor.execute(ISM_query)
        UNPAID = from_cursor.fetchall()


  


        columns = [desc[0] for desc in from_cursor.description]
        UNPAID = pd.DataFrame(UNPAID, columns=columns)
        cols_to_drop = ['TARGET_SETUP_YN']
        UNPAID = UNPAID.drop(columns=cols_to_drop)
        UNPAID['ADATETIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        UNPAID['CHECK_MONTH'] = pd.to_datetime(END_DATE).strftime('%Y-%m') 

        try:
            success, nchunks, nrows, _ = write_pandas(
                conn=to_conn,
                df=UNPAID,
                table_name='BI_UNPAID_CUST',
                database='IVBI',
                schema='MART',
                quote_identifiers=True,
                auto_create_table=True,
                use_logical_type=True
            )
            print("건수 :", nrows)
            overall_row_count += nrows

        except Exception as e:
            print('업로드 오류 발생 Error Message:', e)
            print('오류 발생 시 데이터프레임 샘플:', UNPAID.head())
 
            STATUS = 'X'
            to_cursor.execute(f"""
                INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART 
                VALUES ('BI_UNPAID_CUST', '{start}', NULL, '{STATUS}', '0', 'D', '{str(e)}')
            """)

    try:
        end = datetime.now()  # 종료 시간 기록
        STATUS = 'O'
        ERROR = 'NULL'
        to_cursor.execute(f"""
            INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART 
            VALUES ('BI_UNPAID_CUST', '{start}', '{end}', '{STATUS}', '{overall_row_count}', 'D', {ERROR})
        """)
    except Exception as e:
        end = datetime.now()
        STATUS = 'X'
        to_cursor.execute(f"""
            INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART 
            VALUES ('BI_UNPAID_CUST', '{start}', '{end}', '{STATUS}', '0', 'D', '{str(e)}')
        """)

    to_cursor.close()
    from_cursor.close()
    from_conn.close()
    to_conn.close()

# 데이터 삭제 쿼리
truncate_data_sql = """
TRUNCATE IVBI.MART.BI_UNPAID_CUST
"""

# Airflow Operator 정의
load_data_task = SnowflakeOperator(
    task_id='LOAD_DATA_TO_TARGET_TABLE',
    snowflake_conn_id='snowflake_default_5',
    sql=truncate_data_sql,
    dag=dag,
)

AGG_TABLE_task = PythonOperator(
    task_id='AGG_TABLE',
    python_callable=AGG_TABLE,  # 수정됨: 호출 함수 이름 일치
    dag=dag,
)

load_data_task >> AGG_TABLE_task


    
    
