from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.utils.dates import days_ago
import pandas as pd
from airflow.hooks.dbapi import DbApiHook
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
import pyodbc
import snowflake.connector
import pymssql
from snowflake import connector
import sqlalchemy as sa
from sqlalchemy import create_engine
from typing import Any, Dict, Optional, Tuple, Union
from snowflake.connector import DictCursor, SnowflakeConnection
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect as snowflake_connector
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import os
import csv
from dateutil.relativedelta import relativedelta
import pytz
import re
from datetime import datetime
import pendulum
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, time, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator





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








def check_primary_keys_and_fetch_data(table_name):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)

    try:
        # 주 키 조회 쿼리
        query = f'''
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_NAME = '{table_name}'
            ORDER BY ORDINAL_POSITION;
        '''
        
        cursor.execute(query)
        pk_keys = [key['COLUMN_NAME'] for key in cursor.fetchall()]  # 주 키 리스트로 변환
        pk_key_columns = ', '.join(pk_keys)

    except Exception as e:
        print(f"Error executing query: {e}")
        pk_keys = []
    finally:
        cursor.close()
        conn.close()
    
    return pk_keys


def get_snowflake_data(table_name, pk_keys):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:

        
        pk_columns_str = ', '.join(f'"{key}"' for key in pk_keys)

        query = f"""
        SELECT {pk_columns_str}
        FROM IVBI.ODS.{table_name};
        """

        cursor.execute(query)
        snowflake_data = cursor.fetchall()

        snowflake_df = pd.DataFrame(snowflake_data, columns=[desc[0] for desc in cursor.description])

    except Exception as e:
        print(f"Error executing query: {e}")
        snowflake_df = pd.DataFrame()

    finally:
        cursor.close()
        conn.close()

    return snowflake_df


def delete_snowflake_data(table_name, pk_keys):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    pk_columns_str = ', '.join(pk_keys)

    delete_query = f"""
    DELETE FROM IVBI.ODS.{table_name}
    WHERE MIG_DATETIME IN (
        SELECT MIG_DATETIME
        FROM (
            SELECT
                MIG_DATETIME,
                ROW_NUMBER() OVER (
                    PARTITION BY {pk_columns_str}
                    ORDER BY MIG_DATETIME DESC
                ) AS row_num
            FROM IVBI.ODS.{table_name}
        ) AS SubQuery
        WHERE row_num > 1
    );
    """

    cursor.execute(delete_query)
    conn.commit()
    print(f"{table_name}의 중복된 행 삭제 완료. PK: ({pk_columns_str})")



def run_delete_jobs():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    fetch_query = """
    SELECT DISTINCT(JOB_CODE)
    FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE
    WHERE AGG_ROW_COUNT < 0 
    """
    cursor.execute(fetch_query)
    job_info = cursor.fetchall()

    cursor.close()
    conn.close()

    for job_code_row in job_info:
        job_code = job_code_row[0]
        pk_keys = check_primary_keys_and_fetch_data(job_code)
        if not pk_keys:
            print(f"{job_code}에 대한 PK 키가 정의되어 있지 않습니다. 스킵합니다.")
            continue

        df = get_snowflake_data(job_code, pk_keys)
        print(f"{job_code}: {len(df)}건 PK 조회됨.")
        delete_snowflake_data(job_code, pk_keys)


# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 1),
    'retries': 1
}

dag = DAG(
    'DELETEDATA',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    tags=['snowflake', 'delete', 'dynamic_pk'],
)
    
delete_duplicates = PythonOperator(
        task_id='delete_snowflake_duplicates',
        python_callable=run_delete_jobs,
        dag = dag
    )
    
delete_duplicates

    
