from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pyodbc
import snowflake.connector
import pandas as pd
import pymssql
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

dag = DAG(
    'A2.MSSQL_AGG_TABLE',
    default_args=default_args,
    description='A simple data sync DAG',
    start_date=pendulum.datetime(2024, 10, 9, tz="Asia/Seoul"),
    schedule_interval='45 3 * * 1-7', 
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


SNOWFLAKE_CONN_ID = 'snowflake_default'
MSSQL_CONN_ID = 'mssql_default'
LOG_TABLE = 'LOG_TABLE.MSSQL_AGG_TABLE'  # Snowflake에 집계될 로그 테이블

# Snowflake에서 JOB_CODE 리스트를 가져오는 함수
def get_job_code_list_from_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    query = """
    SELECT JOB_CODE 
    FROM LOG_TABLE.JOB_INFO
    WHERE DEL_YN != 'Y'
    AND AGG_LEVEL = 0
    AND SCHEDULE = 'D'
    AND JOB_CODE IN (
    SELECT JOB_CODE
    FROM LOG_TABLE.JOB_TABLE_INFO
    WHERE UPDATE_COL =  'CAST(RDATETIME AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE) OR CAST(UDATETIME AS DATE) = CAST(DATEADD(day, -1, GETDATE()) AS DATE)')
    """
    cursor.execute(query)
    job_codes = [row[0] for row in cursor.fetchall()]

    cursor.close()
    return job_codes

# MSSQL에서 각 JOB_CODE 테이블의 항목 수를 집계하는 함수
def get_table_row_counts_from_mssql(job_code_list):
    mssql_hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)
    conn = mssql_hook.get_conn()
    cursor = conn.cursor()

    row_counts = []

    for job_code in job_code_list:
        # RDATETIME을 yyyy-MM 형식으로 변환하여 GROUP BY 수행
        query = f"""
        SELECT  FORMAT(RDATETIME, 'yyyy-MM') AS DATE, COUNT(*) AS ROW_COUNT
        FROM {job_code} WITH(NOLOCK)
        WHERE RDATETIME >= DATEADD(month, -12, CONVERT(DATE, CONCAT(YEAR(GETDATE()), '-', MONTH(GETDATE()), '-01')))    
        GROUP BY FORMAT(RDATETIME, 'yyyy-MM') 
        """
        cursor.execute(query)
        
        # 결과를 row_counts 리스트에 저장
        for row in cursor.fetchall():
            row_counts.append({
                'job_code': job_code,
                'date': row[0],  # 'yyyy-MM' 형식의 Month
                'row_count_mssql': row[1],  # 해당 월의 Row Count
            })


    cursor.close()
    return row_counts
# Snowflake에 집계된 데이터를 적재하는 함수
def insert_aggregated_data_to_snowflake(row_counts):
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    for row in row_counts:
        query = f"""
        INSERT INTO {LOG_TABLE} (job_code, date, row_count_mssql)
        VALUES ('{row['job_code']}', '{row['date']}', {row['row_count_mssql']})
        """
        cursor.execute(query)

    conn.commit()
    cursor.close()

# 메인 작업 함수: Job Code를 가져오고, 집계한 후 Snowflake에 저장
def process_job_tables():
    # 1. Snowflake에서 JOB_CODE 리스트 가져오기
    job_code_list = get_job_code_list_from_snowflake()

    # 2. MSSQL에서 해당 JOB_CODE 리스트에 해당하는 테이블들 집계
    row_counts = get_table_row_counts_from_mssql(job_code_list)

    # 3. 집계된 데이터를 Snowflake에 적재
    insert_aggregated_data_to_snowflake(row_counts)

with dag:
    truncate_task = SnowflakeOperator(
        task_id='truncate_mssql_agg_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f'TRUNCATE TABLE {LOG_TABLE};',
        dag=dag
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_mssql_to_snowflake',
        python_callable=process_job_tables,)

    truncate_task >> aggregate_task

