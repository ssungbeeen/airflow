from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
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
import csv
import pytz
import pendulum



class MsSqlHook(DbApiHook):
    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_rds_test'
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
    hook = MsSqlHook(mssql_conn_id='mssql_rds_test')
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


def SNOWFLAKE_CONNECTOR():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default_7')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('USE ROLE ACCOUNTADMIN')
    return conn, cursor


def CHECK_EXIST_TABLE(to_cursor, DATABASE, SCHEMA):
    to_cursor.execute(f"SHOW TABLES IN SCHEMA {DATABASE}.{SCHEMA}")
    existing_tables = to_cursor.fetchall()
    snowflake_tables = [table[1] for table in existing_tables]
    return snowflake_tables


def CREATE_TABLE(from_cursor, to_cursor, DATABASE, SCHEMA, ETL_TABLE):
    data_type_mapping = {
        'int': 'NUMBER',
        'bigint': 'NUMBER',
        'numeric': 'NUMBER',
        'decimal': 'FLOAT',
        'money': 'FLOAT',
        'float': 'FLOAT',
        'real': 'FLOAT',
        'char': 'VARCHAR',
        'nchar': 'VARCHAR',
        'varchar': 'VARCHAR',
        'nvarchar': 'VARCHAR',
        'text': 'TEXT',
        'image': 'TEXT',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'time': 'TIME',
    }

    snowflake_tables = CHECK_EXIST_TABLE(to_cursor, DATABASE, SCHEMA)

    for each_table in ETL_TABLE:
        if each_table not in snowflake_tables:
            table_info_query = f'''
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                CASE WHEN COLUMN_NAME IN (
                        SELECT COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE TABLE_NAME = c.TABLE_NAME
                            AND TABLE_SCHEMA = c.TABLE_SCHEMA
                    ) THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            WHERE TABLE_NAME = '{each_table}'
            '''
            from_cursor.execute(table_info_query)
            table_info = from_cursor.fetchall()

            multiple_PK = "PRIMARY KEY ("
            create_table_query = f"CREATE TABLE {DATABASE}.{SCHEMA}.{each_table} ("
            for col_info in table_info:
                col_name, col_dtype, col_length, numeric_precision, numeric_scale, col_null, col_pk = col_info
                snowflake_data_type = data_type_mapping.get(col_dtype)

                if (col_length == '-1') and (snowflake_data_type not in ['TEXT', 'FLOAT', 'REAL']):
                    if col_length is not None:
                        snowflake_data_type += f"({col_length})"
                    elif numeric_precision is not None:
                        snowflake_data_type += f"({numeric_precision},{numeric_scale})"

                create_table_query += f"{col_name} {snowflake_data_type}"

                if col_null == 'NO':
                    create_table_query += " NOT NULL"
                create_table_query += ","

                if col_pk == 'YES':
                    multiple_PK += f"{col_name},"

            create_table_query += f"MIG_DATETIME TIMESTAMP,"

            if multiple_PK != "PRIMARY KEY (":
                multiple_PK = multiple_PK.rstrip(",") + ")"
                create_table_query = create_table_query + ' ' + multiple_PK + ");"
            else:
                create_table_query = create_table_query.rstrip(",") + ");"

            to_cursor.execute(create_table_query)
            print("SUCCESS JOB : CREATE TABLE ", each_table)
        else:
            print("FAIL JOB : ALREADY EXISTS TABLE ", each_table)


def CHECK_EXIST_COLUMNS(from_cursor, TABLE):
    from_cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{TABLE}'")
    columns_list = from_cursor.fetchall()
    columns_list = [column[0] for column in columns_list]
    return columns_list


def get_check_table_list():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = None

    job_info_query = """
        SELECT *
        FROM IVBI.LOG_TABLE.JOB_INFO
        WHERE AGG_LEVEL = '0'
          AND DEL_YN != 'Y'
          AND SCHEDULE = 'D'
          AND JOB_CODE != 'CAFE24_SITE_ASMANAGE'
    """
    job_info = pd.read_sql(job_info_query, conn)

    return job_info['JOB_CODE'].tolist()



def main_create_only():
    global from_conn, from_cursor, to_conn, to_cursor
    from_conn, from_cursor = MSSQL_CONNECTOR()
    to_conn, to_cursor = SNOWFLAKE_CONNECTOR()

    DATABASE = 'IVBI'
    SCHEMA = 'ODS_TEST'
    CHECK_TABLE = get_check_table_list()

    CREATE_TABLE(from_cursor, to_cursor, DATABASE, SCHEMA, CHECK_TABLE)

    from_conn.close()
    to_conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    'mssql_to_snowflake_create_table_TEST_SCHEMA',
    description='change logic',
    start_date=pendulum.datetime(2024, 12, 9, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
)


run_create_only = PythonOperator(
    task_id='run_create_only',
    python_callable=main_create_only,
    dag = dag
    )


run_create_only
