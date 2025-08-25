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

# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1, 
    'execution_timeout': timedelta(minutes=80)
}

dag = DAG(
    'A.ODS_PIPELINE_CUSTOM',
    default_args=default_args,
    description='A simple data sync DAG',
    start_date=pendulum.datetime(2024, 10, 9, tz="Asia/Seoul"),
    schedule_interval=None, 
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


def read_data(table_name, update_col):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)
    read_table_query = f"SELECT * FROM ISM.dbo.{table_name} WITH(NOLOCK) WHERE {update_col}"
    cursor.execute(read_table_query)
    DATA = cursor.fetchall()
    DATA = pd.DataFrame(DATA)
    if not DATA.empty:
        DATA.columns = [desc[0] for desc in cursor.description]
    return DATA

def check_primary_keys(table_name):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
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
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
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
            query = f"DELETE FROM IVBI.ODS.{table_name} WHERE " + '||'.join(pk_keys) + " IN (" + ','.join(condition) + ")"
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
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
    # DELETE 쿼리 실행
    delete_query = f"""
    DELETE FROM IVBI.ODS.{each_job} 
    WHERE {update_col}
    """
    cursor.execute(delete_query)
    print(f"Executed: {delete_query}")

    cursor.close()
    conn.close()
    
    

def get_column_data_type(table_name, column_name):
    """
    MSSQL에서 컬럼의 데이터 타입을 가져오는 함수.
    """
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)
    
    query = f"""
        SELECT DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}' AND COLUMN_NAME = '{column_name}'
    """
    cursor.execute(query)
    result = cursor.fetchone()
    
    if result:
        return result['DATA_TYPE']
    else:
        raise Exception(f"Column {column_name} not found in table {table_name}")    


def insert_new_data(each_table, DATA, each_job, update_col):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
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
            schema='ODS',
            quote_identifiers=False,
            auto_create_table=False,
            use_logical_type=True
        )

        print(f"{each_table} / Inserted rows: {nrows}")

        row_count_query = f"""
            SELECT COUNT(*) 
            FROM IVBI.ODS.{each_table} 
            WHERE {update_col};
        """
        cursor.execute(row_count_query)
        row_count = cursor.fetchone()[0] if cursor.rowcount != 0 else 0
        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
        log_insert_query = f"""
            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
            (job_code, date, insert_time, row_count, schedule, status) 
            VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {row_count}, 'D', 'O')
        """
        cursor.execute(log_insert_query)
        conn.commit()

    except Exception as e:
        error_message = str(e).replace("'", "''")
        print(f"Error inserting new data for table {each_table}: {error_message}")

        if 'SQL compilation error' in error_message and 'invalid identifier' in error_message:
            match = re.findall(r"''([^']+)''", error_message)
            if match:
                for column_name in match:
                    mssql_data_type = get_column_data_type(each_table, column_name)
                    snowflake_data_type = {
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
                        'time': 'TIME'
                    }.get(mssql_data_type.lower(), 'VARCHAR')

                    try:
                        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
                        add_column_query = f"ALTER TABLE IVBI.ODS.{each_table} ADD COLUMN {column_name} {snowflake_data_type}"
                        cursor.execute(add_column_query)
                        print(f"Added column {column_name} to table {each_table}")
                    except Exception as column_error:
                        print(f"Failed to add column {column_name} for table {each_table}: {column_error}")
                        continue

                try:
                    O, nchunks, nrows, _ = write_pandas(
                        conn=conn,
                        df=DATA,
                        table_name=each_table,
                        schema='ODS',
                        quote_identifiers=False,
                        auto_create_table=False,
                        use_logical_type=True
                    )
                    if not O:
                        raise Exception("Data insertion failed after adding missing column.")
                    print(f"Inserted rows after adding column: {nrows}")

                    cursor.execute(f"""
                        INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                        (job_code, date, insert_time, row_count, schedule, status) 
                        VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), {nrows}, 'D', 'O')
                    """)
                    conn.commit()

                except Exception as log_e:
                    print(f"Error while logging after adding column for {each_job}: {log_e}")
                    raise log_e
            else:
                print("Failed to extract column name from error message.")
                raise e

        else:
            try:
                cursor.execute(f"""
                    INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                    (job_code, date, insert_time, row_count, schedule, status) 
                    VALUES ('{each_table}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0, 'D', 'X')
                """)
                conn.commit()
            except Exception as log_e:
                print(f"Error while logging failure for {each_job}: {log_e}")
            raise e

    finally:
        if cursor:
            cursor.close()
        conn.close()


                
job_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_INFO WHERE AGG_LEVEL = '0' AND DEL_YN != 'Y' AND SCHEDULE = 'D' AND (JOB_CODE = 'BR_TLS_TRAGE_AGREE')"
update_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_TABLE_INFO WHERE DEL_YN != 'Y'"

def process_jobs(**context):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = None
    completed_jobs = set() 
    try:
        cursor = conn.cursor()

        job_info = pd.read_sql(job_info_query, conn)
        ods_job_list = job_info['JOB_CODE'].tolist()
        update_info = pd.read_sql(update_info_query, conn)

        for each_job in ods_job_list:
            if each_job in completed_jobs:
                print(f"Skipping already completed job: {each_job}")
                continue
            try:
                # UPDATE_COL 정보 추출
                each_update_col = update_info[update_info['JOB_CODE'] == each_job]['UPDATE_COL']
                if len(each_update_col) == 1:
                    update_col = each_update_col.iloc[0]  # UPDATE_COL 값을 추출
                    DATA = read_data(each_job, update_col)      
                    if len(DATA) != 0:
                        print(f"Processing data for table {each_job}, {len(DATA)} rows retrieved with {update_col}")
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
                        # 데이터가 없을 경우 로그 기록
                        print(f"No data to process for table {each_job} with {update_col}")
                        cursor.execute(f"""
                            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                            (job_code, date, insert_time, row_count, schedule, status) 
                            VALUES ('{each_job}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0, 'D', 'O')
                        """)
                        conn.commit()
                        completed_jobs.add(each_job)
                else:
                    cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
                    print(f"No update column found for table {each_job}")
                    log_insert_query = f"""
                            INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                            (job_code, date, insert_time, row_count, schedule, status) 
                            VALUES ('{each_job}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0, 'D', 'O')
                        """
                    cursor.execute(log_insert_query)
                    conn.commit()
                    completed_jobs.add(each_job)

            except Exception as e:
                # 작업 도중 에러 발생 시 로그 기록 및 출력
                print(f"Error processing job {each_job}: {e}")
                cursor.execute(f"""
                    INSERT INTO LOG_TABLE.JOB_TABLE_LOG 
                    (job_code, date, insert_time, row_count, schedule, status) 
                    VALUES ('{each_job}', CURRENT_DATE(), CURRENT_TIMESTAMP(), 0, 'D', 'X')
                """)
                conn.commit()

    finally:
        # 커넥션 및 커서 종료
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("All connections closed.")
        
        
process_jobs_task = PythonOperator(
    task_id='ODS_JOB',
    python_callable=process_jobs,
    provide_context=True,
    dag=dag,
)

process_jobs_task
