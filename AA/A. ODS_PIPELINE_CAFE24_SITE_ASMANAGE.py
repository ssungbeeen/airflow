from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pyodbc
import snowflake.connector
import pandas as pd
import pymssql
from airflow.utils.dates import days_ago
from snowflake import connector
import time
from airflow.hooks.dbapi import DbApiHook
import sqlalchemy as sa
from sqlalchemy import create_engine
from typing import Any, Dict, Optional, Tuple, Union
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect as snowflake_connector
import os
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import csv
import pytz
import re
from datetime import datetime
import pendulum

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1, 
    'execution_timeout': timedelta(minutes=80)
}

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

def MSSQL_CONNECTOR():
    hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor



def SNOWFLAKE_CONNECTOR():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('USE ROLE ACCOUNTADMIN')
    return conn, cursor



def SELECT_MSSQL_VIEW(from_cursor, view_name):
    print(f"START JOB: READ VIEW {view_name}")
    select_view_query = f"SELECT * FROM ISM.dbo.{view_name} WITH(NOLOCK)"
    from_cursor.execute(select_view_query)
    DATA = from_cursor.fetchall()
    DATA = pd.DataFrame(DATA, columns=[desc[0] for desc in from_cursor.description])
    print(f"건수: {len(DATA)}")
    return DATA



def COPY_SNOWFLAKE_TABLE(to_conn, to_cursor, DATABASE, SCHEMA, table_name, DATA):
    print(f"START JOB: INSERT TABLE {table_name}")
    start = time.time()
    DATA['MIG_DATETIME'] = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        success, nchunks, nrows, _ = write_pandas(conn=to_conn,
                                                  df=DATA,
                                                  table_name=table_name,
                                                  database=DATABASE,
                                                  schema=SCHEMA,
                                                  quote_identifiers=False,
                                                  auto_create_table=False,
                                                  use_logical_type=True)
                                                  
        print(f"건수: {nrows}")                                                                                                    
                                                  
        row_count_query = f"""
            SELECT COUNT(*) 
            FROM IVBI.ODS.{table_name};
        """
        to_cursor.execute(row_count_query)
        row_count = to_cursor.fetchone()[0] if to_cursor.rowcount != 0 else 0
        to_cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
        log_insert_query = f"""
            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
            (job_code, date, insert_time, row_count,schedule, status) 
            VALUES ('{table_name}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {row_count},'D', 'O')
        """
        to_cursor.execute(log_insert_query)
        to_conn.commit()
                                          

    except Exception as e:
        print(f"Error: {e}")

    end = time.time()
    print(f"END JOB: {timedelta(seconds=(end - start))}")



def main():
    from_conn, from_cursor = MSSQL_CONNECTOR()
    to_conn, to_cursor = SNOWFLAKE_CONNECTOR()

    DATABASE = 'IVBI'
    SCHEMA = 'ODS'
    VIEW_NAME = 'CAFE24_SITE_ASMANAGE'  # VIEW 이름을 지정





    # VIEW에서 데이터를 추출하고 Snowflake에 복사
    DATA = SELECT_MSSQL_VIEW(from_cursor, VIEW_NAME)
    COPY_SNOWFLAKE_TABLE(to_conn, to_cursor, DATABASE, SCHEMA, VIEW_NAME, DATA)

    from_conn.close()
    to_conn.close()




with DAG(
    'A.ODS_PIPELINE_CAFE24_SITE_ASMANAGE',
    default_args=default_args,
    description='ETL MSSQL VIEW to Snowflake',
    start_date=pendulum.datetime(2024, 10, 23, tz="Asia/Seoul"),
    schedule_interval='55 2 * * 1-7',
    catchup=False,
) as dag:
    truncate_task = SnowflakeOperator(
        task_id='truncate_ods_table',
        snowflake_conn_id='snowflake_default',
        sql=f'TRUNCATE TABLE ODS.CAFE24_SITE_ASMANAGE;',
        dag=dag
    )
    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=main,
    )

    truncate_task >> run_etl
