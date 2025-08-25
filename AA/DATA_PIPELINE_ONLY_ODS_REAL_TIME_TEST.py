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
import re
from datetime import datetime
import pendulum

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0, 
    'execution_timeout': timedelta(minutes=60)
}

dag = DAG(
    'REAL_TIME_HOURLY_ODS_TEST__2',
    default_args=default_args,
    description='A simple data sync DAG',
    start_date=pendulum.datetime(2024, 8, 23, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False,
)
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


def read_data(table_name, update_col):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_rds_test')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)  # 결과를 딕셔너리로 반환
    read_table_query = f"SELECT * FROM ISM.dbo.{table_name} WITH(NOLOCK) WHERE {update_col}"
    cursor.execute(read_table_query)
    DATA = cursor.fetchall()

    # 결과가 튜플이면 DataFrame으로 변환할 때 발생할 수 있는 문제를 방지
    if DATA:
        DATA = pd.DataFrame(DATA)
        # 열 이름을 설정할 때 오류가 발생하는지 확인 필요
        try:
            DATA.columns = [desc[0] for desc in cursor.description]
        except Exception as col_error:
            print(f"Error assigning column names: {col_error}")
            DATA.columns = range(len(DATA.columns))  # 컬럼이름 할당 실패 시 기본 인덱스 할당
    
    return DATA

def check_primary_keys(table_name):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_rds_test')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)
    query = f'''SELECT COLUMN_NAME
                 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
                      AND TABLE_NAME = '{table_name}'
                ORDER BY ORDINAL_POSITION'''
    cursor.execute(query)
    pk_keys = cursor.fetchall()
    pk_keys = [key['COLUMN_NAME'] for key in pk_keys]
    return pk_keys

def delete_old_data(table_name, pk_keys, del_data):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_7')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    batch_size = 100000
    num_batches = (len(del_data) // batch_size) + 1

    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min((batch_num + 1) * batch_size, len(del_data))
        batch_data = del_data.iloc[batch_start:batch_end]
        condition = ["'"+''.join(map(str, row))+"'" for _, row in batch_data.iterrows()]
        
        if condition:
            query = f"DELETE FROM IVBI.ODS_TEST.{table_name} WHERE " + '||'.join(pk_keys) + " IN (" + ','.join(condition) + ")"
            try:
                cursor.execute(query)
                conn.commit()
            except Exception as ERROR:
                print(f"Error deleting old data for table {table_name}, batch {batch_num + 1}: {ERROR}")
        else:
            print(f"No data to delete in batch {batch_num + 1}")

    cursor.close()
    conn.close()
    
def delete_data(update_col, each_job):
    # Snowflake와의 연결 설정
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_7')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
    # DELETE 쿼리 실행
    delete_query = f"""
    DELETE FROM IVBI.ODS_TEST.{each_job} 
    WHERE {update_col}
    """
    cursor.execute(delete_query)
    print(f"Executed: {delete_query}")

    cursor.close()
    conn.close()




def insert_new_data(each_table, DATA, each_job, update_col):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_7')
    conn = snowflake_hook.get_conn()
    cursor = None
    DATA['MIG_DATETIME'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        cursor = conn.cursor()
        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")


        O, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=DATA,
            table_name=each_table,
            database='IVBI',
            schema='ODS_TEST',
            quote_identifiers=False,
            auto_create_table=False,
            use_logical_type=True
        )
        
        print(f"{each_table} / Inserted rows: {nrows}")

        row_count_query = f"""
            SELECT COUNT(*) 
            FROM IVBI.ODS_TEST.{each_table} 
            WHERE {update_col};
        """
        cursor.execute(row_count_query)
        row_count = cursor.fetchone()[0] if cursor.rowcount != 0 else 0
        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
        log_insert_query = f"""
            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
            (job_code,date, insert_time, row_count,schedule ,status) 
            VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {row_count},'H','O')
        """
        cursor.execute(log_insert_query)
        conn.commit()

    except Exception as e:
        error_message = str(e).replace("'", "''")
        print(f"Error inserting new data for table {each_table}: {error_message}")

        try:
            cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
            if 'SQL compilation error' in error_message:
                if 'invalid identifier' in error_message:
                    match = re.search(r"''([^']+)''", error_message)
                    if match:
                        column_name = match.group(1)
                        add_column_query = f"ALTER TABLE IVBI.ODS_TEST.{each_table} ADD COLUMN {column_name} VARCHAR"
                        cursor.execute(add_column_query)                        

                        print(f"Added column {column_name} to table {each_table}")

                        O, nchunks, nrows, _ = write_pandas(
                            conn=conn,
                            df=DATA,
                            table_name=each_table,
                            schema='ODS_TEST',
                            quote_identifiers=False,
                            auto_create_table=False,
                            use_logical_type=True
                        )
                        if not O:
                            raise Exception("Data insertion failed after adding missing column.")
                        print(f"Inserted rows after adding column: {nrows}")

                        cursor.execute(f"""
                            INSERT INTO LOG_TABLE.JOB_TABLE_LOG (job_code, date, insert_time, row_count,schedule,status) 
                            VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {nrows},'H', 'O')
                        """)	    
                    else:
                        raise Exception("Failed to extract column name from error message.")
                else:
                    cursor.execute(f"""
                        INSERT INTO LOG_TABLE.JOB_TABLE_LOG (job_code, date, insert_time, row_count,schedule, status) 
                        VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {nrows},'H', 'O')
                    """)

            else:
                cursor.execute(f"""
                    INSERT INTO LOG_TABLE.JOB_TABLE_LOG (job_code, date, insert_time, row_count,schedule, status) 
                    VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0,'H', 'X')
                """)

            conn.commit()


        except Exception as log_e:
            print(f"Error while logging X for {each_job}: {log_e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        conn.close()
                
job_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_INFO WHERE AGG_LEVEL = '0' AND DEL_YN != 'Y' AND SCHEDULE = 'D' AND (JOB_CODE != 'TLS_TAX_CLOSE' AND JOB_CODE != 'CAFE24_SITE_ASMANAGE')"
update_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_TABLE_INFO WHERE DEL_YN != 'Y'"

def process_jobs(**context):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_7')
    conn = snowflake_hook.get_conn()
    cursor = None
    try:
        cursor = conn.cursor()
        job_info = pd.read_sql(job_info_query, conn)
        ods_job_list = job_info['JOB_CODE'].tolist()
        update_info = pd.read_sql(update_info_query, conn)

        for each_job in ods_job_list:
            try:
                each_update_col = update_info[update_info['JOB_CODE'] == each_job]['UPDATE_COL']

                if len(each_update_col) == 1:
                    update_col = each_update_col.iloc[0]  # UPDATE_COL 값을 추출
                    DATA = read_data(each_job, update_col)
                    if len(DATA) != 0:
                        pk_keys = check_primary_keys(each_job)
                        
                        if 'UDATETIME' in DATA.columns:
                            del_data = DATA[~DATA['UDATETIME'].isna()][pk_keys]
                            delete_old_data(each_job, pk_keys, del_data)
                            delete_data(update_col, each_job)
                            insert_new_data(each_job, DATA, each_job, update_col)
                        else:
                            del_data = pd.DataFrame(columns=pk_keys)
                            delete_data(update_col, each_job)
                            delete_old_data(each_job, pk_keys, del_data)
                            insert_new_data(each_job, DATA, each_job, update_col)  

                    else:
                        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
                        print(f"No data found for table {each_job} to process.")
                        log_insert_query = f"""
                            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                            (job_code, date, insert_time, row_count,schedule, status) 
                            VALUES ('{each_job}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0,'H', 'O')
                        """
                        cursor.execute(log_insert_query)
                        conn.commit()
            except Exception as e:
                print(f"Error processing job {each_job}: {e}")
    finally:
        if cursor:
            cursor.close()
        conn.close()

process_jobs_task = PythonOperator(
    task_id='TEST_PIPELINE_REAL_TIME_JOB',
    python_callable=process_jobs,
    provide_context=True,
    dag=dag,
)

process_jobs_task
