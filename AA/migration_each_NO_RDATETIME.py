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
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default_test')
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


def SELECT_MSSQL_TABLE_BY_YEAR(from_cursor, each_table, start_year, end_year):
    print("START JOB : READ TABLE", each_table)
    DATA = pd.DataFrame()

    columns_list = CHECK_EXIST_COLUMNS(from_cursor, each_table)

    if 'RDATETIME' in columns_list:
        # RDATETIME 컬럼이 존재할 경우, 연도별로 데이터를 가져옴
        for year in range(start_year, end_year + 1):
            print(f"Extracting data for year: {year}")
            start_date = datetime(year, 1, 1)
            end_date = datetime(year + 1, 1, 1)

            select_table_query = f"""
                SELECT * 
                FROM ISM.dbo.{each_table} WITH(NOLOCK)
                WHERE (RDATETIME >= '{start_date.strftime('%Y-%m-%d')}') AND (RDATETIME < '{end_date.strftime('%Y-%m-%d')}')
            """
            from_cursor.execute(select_table_query)
            yearly_data = from_cursor.fetchall()
            yearly_data = pd.DataFrame(yearly_data, columns=[desc[0] for desc in from_cursor.description])
            DATA = pd.concat([DATA, yearly_data], ignore_index=True)
    else:
        # RDATETIME 컬럼이 없을 경우, 전체 데이터를 가져옴
        print(f"RDATETIME not exists in Table '{each_table}'")
        select_table_query = f"SELECT * FROM ISM.dbo.{each_table} WITH(NOLOCK)"
        from_cursor.execute(select_table_query)
        data = from_cursor.fetchall()
        data = pd.DataFrame(data, columns=[desc[0] for desc in from_cursor.description])
        DATA = pd.concat([DATA, data], ignore_index=True)

    print("건수 : ", len(DATA))
    return DATA


def COPY_SNOWFLAKE_TABLE(to_conn, to_cursor, DATABASE, SCHEMA, each_table, DATA):
    print("START JOB : INSERT TABLE", each_table)
    start = time.time()
    DATA['MIG_DATETIME'] = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        success, nchunks, nrows, _ = write_pandas(conn=to_conn,
                                                  df=DATA,
                                                  table_name=each_table,
                                                  database=DATABASE,
                                                  schema=SCHEMA,
                                                  quote_identifiers=False,
                                                  auto_create_table=False,
                                                  use_logical_type=True)
        print("건수 : ", nrows)
    except Exception as e:
        retry_copy.append(each_table)
        print(f"Error: {e}")

    end = time.time()
    print("END JOB : ", timedelta(seconds=(end - start)))


def main():
    global from_conn, from_cursor, to_conn, to_cursor, retry_copy, retry_read
    from_conn, from_cursor = MSSQL_CONNECTOR()
    to_conn, to_cursor = SNOWFLAKE_CONNECTOR()

    DATABASE = 'IVBI'
    SCHEMA = 'ODS'
    CHECK_TABLE = ['BR_TLS_APP_PUSH']

    retry_copy = []
    retry_read = []

    CREATE_TABLE(from_cursor, to_cursor, DATABASE, SCHEMA, CHECK_TABLE)

    start_year = 2023
    end_year = 2025

    for each_table in CHECK_TABLE:
        print("시작 : ", datetime.now())
        for year in range(start_year, end_year + 1):
            DATA = SELECT_MSSQL_TABLE_BY_YEAR(from_cursor, each_table, year, year)
            COPY_SNOWFLAKE_TABLE(to_conn, to_cursor, DATABASE, SCHEMA, each_table, DATA)
        print("종료 : ", datetime.now())
        print("==================================================")

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

with DAG(
    'mssql_to_snowflake_etl_each_table_NO_RDATETIME',
    default_args=default_args,
    description='ETL from MSSQL to Snowflake',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_etl = PythonOperator(
        task_id='run_etl',
        python_callable=main,
    )

    run_etl
