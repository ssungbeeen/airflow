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



# DAG 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1
}

dag = DAG(
    'A4.COMPARE_DATE_AND_DELETE_DATE_EDIT_QUERY',
    default_args=default_args,
    description='Insert rows with different row counts into DELETE_JOB_CODE_TABLE',
    start_date=pendulum.datetime(2024, 10, 17, tz="Asia/Seoul"),
    schedule_interval='52 3 * * 1-7',
    catchup=False
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




SNOWFLAKE_CONN_ID = 'snowflake_default_2'
MSSQL_AGG_TABLE = 'IVBI.LOG_TABLE.MSSQL_AGG_TABLE'
SNOWFLAKE_AGG_TABLE = 'IVBI.LOG_TABLE.SNOWFLAKE_AGG_TABLE'
DELETE_JOB_CODE_AGG_TABLE = 'IVBI.LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE'
JOB_CODE_AGG_TABLE = 'IVBI.LOG_TABLE.JOB_CODE_AGG_TABLE'

# MSSQL_AGG_TABLE과 SNOWFLAKE_AGG_TABLE에서 데이터를 읽어오는 함수_O
def get_agg_table_data():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # MSSQL_AGG_TABLE 데이터 가져오기
    query_mssql = f"SELECT JOB_CODE, DATE, ROW_COUNT_MSSQL FROM {MSSQL_AGG_TABLE}"
    cursor.execute(query_mssql)
    mssql_data = cursor.fetchall()
    mssql_df = pd.DataFrame(mssql_data, columns=['JOB_CODE', 'DATE', 'ROW_COUNT_MSSQL'])

    # SNOWFLAKE_AGG_TABLE 데이터 가져오기
    query_snowflake = f"SELECT JOB_CODE, DATE, ROW_COUNT_SNOWFLAKE FROM {SNOWFLAKE_AGG_TABLE}"
    cursor.execute(query_snowflake)
    snowflake_data = cursor.fetchall()
    snowflake_df = pd.DataFrame(snowflake_data, columns=['JOB_CODE', 'DATE', 'ROW_COUNT_SNOWFLAKE'])

    cursor.close()
    return mssql_df, snowflake_df


# ROW_COUNT가 다른 경우만 DELETE_JOB_CODE_TABLE에 삽입하는 함수_차이가 나는경우
def insert_all_to_delete_job_code_table(**kwargs):
    mssql_df, snowflake_df = get_agg_table_data()

    # 두 테이블을 JOB_CODE와 DATE 기준으로 병합
    merged_df = pd.merge(mssql_df, snowflake_df, on=['JOB_CODE', 'DATE'])

    # DELETE_COUNT 계산 (ROW_COUNT_MSSQL - ROW_COUNT_SNOWFLAKE)
    merged_df['AGG_ROW_COUNT'] = merged_df['ROW_COUNT_MSSQL'] - merged_df['ROW_COUNT_SNOWFLAKE']

    merged_df['STATUS'] = merged_df['AGG_ROW_COUNT'].apply(
        lambda x: '1차처리 완료' if x == 0 else '1차처리 필요'
    )    


    if not merged_df.empty:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # 차이나는 항목을 DELETE_JOB_CODE_AGG_TABLE에 삽입
        for _, row in merged_df.iterrows():
            job_code = row['JOB_CODE']
            date = row['DATE']
            row_count_mssql = row['ROW_COUNT_MSSQL']
            row_count_snowflake = row['ROW_COUNT_SNOWFLAKE']
            agg_row_count = row['AGG_ROW_COUNT']
            status_row_count = row['STATUS']

            insert_query = f"""
            INSERT INTO {JOB_CODE_AGG_TABLE} (JOB_CODE, DATE,ROW_COUNT_MSSQL, ROW_COUNT_SNOWFLAKE,AGG_ROW_COUNT,STATUS)
            VALUES ('{job_code}', '{date}', '{row_count_mssql}','{row_count_snowflake}','{agg_row_count}','{status_row_count}')
            """
            cursor.execute(insert_query)




def insert_diff_to_delete_job_code_table(**kwargs):
    mssql_df, snowflake_df = get_agg_table_data()

    # 두 테이블을 JOB_CODE와 DATE 기준으로 병합
    merged_df = pd.merge(mssql_df, snowflake_df, on=['JOB_CODE', 'DATE'])

    # DELETE_COUNT 계산 (ROW_COUNT_MSSQL - ROW_COUNT_SNOWFLAKE)
    merged_df['AGG_ROW_COUNT'] = merged_df['ROW_COUNT_MSSQL'] - merged_df['ROW_COUNT_SNOWFLAKE']

    # STATUS 컬럼 추가
    if 'AGG_ROW_COUNT' in merged_df.columns:
        merged_df['STATUS'] = merged_df['AGG_ROW_COUNT'].apply(
            lambda x: '1차처리 완료' if x == 0 else '1차처리 필요'
        )
    else:
        print("AGG_ROW_COUNT 컬럼이 없습니다.")

    # DELETE_COUNT가 양수이거나 음수인 경우만 필터링 (삭제할 데이터가 있는 경우)
    diff_df = merged_df[merged_df['ROW_COUNT_MSSQL'] != merged_df['ROW_COUNT_SNOWFLAKE']]

    if not diff_df.empty:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # 차이나는 항목을 DELETE_JOB_CODE_AGG_TABLE에 삽입
        for _, row in diff_df.iterrows():
            job_code = row['JOB_CODE']
            date = row['DATE']
            row_count_mssql = row['ROW_COUNT_MSSQL']
            row_count_snowflake = row['ROW_COUNT_SNOWFLAKE']
            agg_row_count = row['AGG_ROW_COUNT']
            status = row['STATUS']

            insert_query = f"""
            INSERT INTO {DELETE_JOB_CODE_AGG_TABLE} (JOB_CODE, DATE, ROW_COUNT_MSSQL, ROW_COUNT_SNOWFLAKE, AGG_ROW_COUNT, STATUS)
            VALUES ('{job_code}', '{date}', '{row_count_mssql}', '{row_count_snowflake}', '{agg_row_count}', '{status}')
            """
            cursor.execute(insert_query)

        conn.commit()
        cursor.close()




def execute_email_all(**kwargs):

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
    table_query = """SELECT job_code,date,row_count_mssql,row_count_snowflake,agg_row_count FROM JOB_CODE_AGG_TABLE ;"""
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(table_query)
    columns = [col[0] for col in cursor.description]
    
    # Fetch the results as a list of tuples
    results = cursor.fetchall()
    
    # Create a DataFrame from the results
    df = pd.DataFrame(results, columns=columns)
    
    cursor.close()
    conn.close()
    email_content = df.to_html(index=False)    

    send_first_email_task = EmailOperator(
        task_id='send_email',
        to='IVBI@eland.co.kr',
        subject=f"DELETE_집계 테이블 현황_{datetime.now().strftime('%Y%m%d')}",
        html_content=email_content,
        dag=dag,
    )
    send_first_email_task.execute(context=kwargs)


truncate_task = SnowflakeOperator(
    task_id='truncate_JOB_CODE_AGG_TABLE',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f'TRUNCATE TABLE LOG_TABLE.JOB_CODE_AGG_TABLE;',
    dag=dag
)

truncate_task_agg_table = SnowflakeOperator(
    task_id='truncate_DELETE_JOB_CODE_AGG_TABLE',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f'TRUNCATE TABLE LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE;',
    dag=dag
)
insert_task = PythonOperator(
    task_id='insert_all_to_delete_job_code_table',
    python_callable=insert_all_to_delete_job_code_table,
    provide_context=True,
    dag=dag
)


insert_other_task = PythonOperator(
    task_id='insert_diff_to_delete_job_code_table',
    python_callable=insert_diff_to_delete_job_code_table,
    provide_context=True,
    dag=dag
)




execute_and_first_email_task = PythonOperator(
    task_id='execute_and_email_first',
    python_callable=execute_email_all,
    retries = 9,
    provide_context=True,  # This is important to provide the context to the Python function
    dag=dag,
)

def get_table_names_and_dates_from_snowflake():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        query = "SELECT JOB_CODE, DATE FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE"
        cursor.execute(query)
        results = cursor.fetchall()
        
        # 튜플 형식의 데이터를 딕셔너리 형태로 변환
        table_data = [{'JOB_CODE': row[0], 'DATE': row[1]} for row in results]
    
    except Exception as e:
        print(f"Error fetching table names and dates from Snowflake: {e}")
        table_data = []
    
    finally:
        cursor.close()
        conn.close()
    
    return table_data


def check_primary_keys_and_fetch_data(table_name, date_value):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)

    end_date = datetime.today().replace(day=1) + relativedelta(months=1)
    start_date = end_date.replace(year=end_date.year - 1, month=end_date.month) - relativedelta(months=12)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
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
        
        # RDATETIME을 YYYY-MM 형식으로 변환하여 비교
        pk_key_columns = ', '.join(pk_keys)  # 주 키 문자열 변환
        query_data = f'''
            SELECT {pk_key_columns}
            FROM [{table_name}] WITH(NOLOCK)
            WHERE CONVERT(DATE, RDATETIME, 23) >= '{start_date_str}' AND CONVERT(DATE, RDATETIME, 23) < '{end_date_str}'

        ''' 
        
        cursor.execute(query_data)
        mssql_data = cursor.fetchall()

    except Exception as e:
        print(f"Error executing query: {e}")
        pk_keys = []
        mssql_data = []
    finally:
        cursor.close()
        conn.close()
    
    return pk_keys, mssql_data


def get_snowflake_data(table_name, date_value, pk_keys):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        # PK 키를 문자열로 변환 (대괄호 추가)
        pk_keys_str = ', '.join(f'"{key}"' for key in pk_keys) 

        # Snowflake에서 데이터 조회 쿼리
        query = f"""
        SELECT {pk_keys_str}
        FROM IVBI.ODS.{table_name}
        WHERE TO_CHAR(RDATETIME, 'yyyy-MM') = '{date_value}';
        """
        
        cursor.execute(query)
        snowflake_data = cursor.fetchall()
        
        # Snowflake에서 조회한 데이터를 DataFrame으로 변환
        snowflake_df = pd.DataFrame(snowflake_data, columns=[desc[0] for desc in cursor.description])
    
    except Exception as e:
        print(f"Error executing query: {e}")
        snowflake_df = pd.DataFrame()
    
    finally:
        cursor.close()
        conn.close()
    
    return snowflake_df



def compare_and_delete():
    # Snowflake에서 JOB_CODE와 DATE 값을 가져옴
    table_data = get_table_names_and_dates_from_snowflake()
    
    for table in table_data:
        table_name = table['JOB_CODE']
        date_value = table['DATE']
        
        # 주 키를 확인하고 RDATETIME 기준으로 데이터를 조회
        pk_keys, mssql_data = check_primary_keys_and_fetch_data(table_name, date_value)
        
        if pk_keys:
            mssql_df = pd.DataFrame(mssql_data, columns=pk_keys)
            snowflake_df = get_snowflake_data(table_name, date_value, pk_keys)
            
            # PK 컬럼을 문자열로 합쳐서 비교
            mssql_df['PK_COMBINED'] = mssql_df[pk_keys].astype(str).agg('|'.join, axis=1)
            snowflake_df['PK_COMBINED'] = snowflake_df[pk_keys].astype(str).agg('|'.join, axis=1)
            
            # MSSQL에는 없고 Snowflake에만 있는 PK 찾기
            diff_df = snowflake_df[~snowflake_df['PK_COMBINED'].isin(mssql_df['PK_COMBINED'])]
            
            if not diff_df.empty:
                delete_snowflake_data(diff_df, table_name, pk_keys)
            else:
                print(f"No data to delete for {table_name} on {date_value}")


def delete_snowflake_data(diff_df, table_name, pk_keys):
    # Snowflake에서 MSSQL에 없는 데이터를 삭제하는 함수
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # PK 컬럼을 문자열로 합쳐서 비교 (PK 값)
    pk_columns_str = ', '.join(pk_keys)
    pk_list = diff_df[pk_keys].astype(str).agg('|'.join, axis=1).tolist()
    
    # Snowflake에서 삭제할 PK 리스트 생성
    pk_placeholders = ', '.join(['%s'] * len(pk_keys))
    
    # 삭제 쿼리 생성
    delete_query = f"""
    DELETE FROM IVBI.ODS.{table_name}
    WHERE ( {pk_columns_str} ) IN ( {', '.join(['(' + pk_placeholders + ')'] * len(pk_list))} )
    """
    
    # 쿼리에 사용할 PK 값들을 단일 리스트로 평탄화
    flat_pk_values = [item for sublist in diff_df[pk_keys].values.tolist() for item in sublist]

    cursor.execute(delete_query, flat_pk_values)
    conn.commit()
    
    print(f"Deleted {len(diff_df)} rows from Snowflake")

def process_tables():
    # Snowflake에서 JOB_CODE와 DATE 값을 가져옴
    table_data = get_table_names_and_dates_from_snowflake()
    
    for table in table_data:
        table_name = table['JOB_CODE']  # JOB_CODE 가져오기
        date_value = table['DATE']  # DATE를 'YYYY-MM' 형식으로 변환
        
        # 주 키를 확인하고 RDATETIME 기준으로 데이터를 조회하는 함수 호출
        pk_keys, ms_data = check_primary_keys_and_fetch_data(table_name, date_value)
        
        if pk_keys:
            snowflake_df = get_snowflake_data(table_name, date_value, pk_keys)
            print(f"Primary keys for {table_name} on {date_value}: {pk_keys}")
            print(f"Snowflake DataFrame:\n{snowflake_df}")






######SNOWFLAKE 재집계

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
    FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE)
    
    """
    cursor.execute(query)
    job_codes = [row[0] for row in cursor.fetchall()]

    cursor.close()
    return job_codes

# Snowflake에서 각 JOB_CODE 테이블의 항목 수를 집계하는 함수
def get_table_row_counts_from_snowflake(job_code_list):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    row_counts = []

    for job_code in job_code_list:
        # RDATETIME을 yyyy-MM 형식으로 변환하여 GROUP BY 수행
        query = f"""
        SELECT TO_CHAR(RDATETIME, 'yyyy-MM') AS DATE, COUNT(*) AS ROW_COUNT
        FROM ODS.{job_code}
        WHERE RDATETIME >= DATEADD(month, -12, DATE_TRUNC('month', CURRENT_DATE))
        GROUP BY TO_CHAR(RDATETIME, 'yyyy-MM')
        """
        cursor.execute(query)
        
        # 결과를 row_counts 리스트에 저장
        for row in cursor.fetchall():
            row_counts.append({
                'job_code': job_code,
                'date': row[0],  # 'yyyy-MM' 형식의 Month
                'row_count_snowflake': row[1],  # 해당 월의 Row Count
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
        INSERT INTO LOG_TABLE.SNOWFLAKE_AGG_TABLE (job_code, date, row_count_snowflake)
        VALUES ('{row['job_code']}', '{row['date']}', {row['row_count_snowflake']})
        """
        cursor.execute(query)

    conn.commit()
    cursor.close()

# 메인 작업 함수: Job Code를 가져오고, 집계한 후 Snowflake에 저장
def process_job_tables_snowflake():
    # 1. Snowflake에서 JOB_CODE 리스트 가져오기
    job_code_list = get_job_code_list_from_snowflake()

    # 2. Snowflake에서 해당 JOB_CODE 리스트에 해당하는 테이블들 집계
    row_counts = get_table_row_counts_from_snowflake(job_code_list)

    # 3. 집계된 데이터를 Snowflake에 적재
    insert_aggregated_data_to_snowflake(row_counts)



truncate_snowflake_task = SnowflakeOperator(
    task_id='truncate_snowflake_agg_table',
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=f'TRUNCATE TABLE LOG_TABLE.SNOWFLAKE_AGG_TABLE;',
    dag=dag
)


aggregate_task = PythonOperator(
    task_id='aggregate_snowflake_data',
    python_callable=process_job_tables_snowflake,
    dag=dag
)

def get_agg_table_data_agg():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # MSSQL_AGG_TABLE 데이터 가져오기
    query_mssql = f"SELECT m.JOB_CODE, m.DATE, m.ROW_COUNT_MSSQL FROM LOG_TABLE.MSSQL_AGG_TABLE m WHERE (m.JOB_CODE, m.DATE) IN (SELECT d.JOB_CODE, d.DATE FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE d);"
    cursor.execute(query_mssql)
    mssql_data = cursor.fetchall()
    mssql_df = pd.DataFrame(mssql_data, columns=['JOB_CODE', 'DATE', 'ROW_COUNT_MSSQL'])

    # SNOWFLAKE_AGG_TABLE 데이터 가져오기
    query_snowflake = f"SELECT m.JOB_CODE, m.DATE, m.ROW_COUNT_SNOWFLAKE FROM LOG_TABLE.SNOWFLAKE_AGG_TABLE m WHERE (m.JOB_CODE, m.DATE) IN (SELECT d.JOB_CODE, d.DATE FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE d);"
    cursor.execute(query_snowflake)
    snowflake_data = cursor.fetchall()
    snowflake_df = pd.DataFrame(snowflake_data, columns=['JOB_CODE', 'DATE', 'ROW_COUNT_SNOWFLAKE'])

    cursor.close()
    return mssql_df, snowflake_df









def insert_diff_to_delete_job_code_table_2(**kwargs):
    mssql_df, snowflake_df = get_agg_table_data_agg()

    # 두 테이블을 JOB_CODE와 DATE 기준으로 병합
    merged_df = pd.merge(mssql_df, snowflake_df, on=['JOB_CODE', 'DATE'])

    # DELETE_COUNT 계산 (ROW_COUNT_MSSQL - ROW_COUNT_SNOWFLAKE)
    merged_df['AGG_ROW_COUNT'] = merged_df['ROW_COUNT_MSSQL'] - merged_df['ROW_COUNT_SNOWFLAKE']



    if 'AGG_ROW_COUNT' in merged_df.columns:
        merged_df['STATUS'] = merged_df['AGG_ROW_COUNT'].apply(
            lambda x: '2차처리 완료' if x == 0 else '당일 데이터 미반영'
        )
    else:
        print("AGG_ROW_COUNT 컬럼이 없습니다.")



    if not merged_df.empty:
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # 차이나는 항목을 DELETE_JOB_CODE_AGG_TABLE에 삽입
        for _, row in merged_df.iterrows():
            job_code = row['JOB_CODE']
            date = row['DATE']
            row_count_mssql = row['ROW_COUNT_MSSQL']
            row_count_snowflake = row['ROW_COUNT_SNOWFLAKE']
            agg_row_count = row['AGG_ROW_COUNT']
            status_row_count = row['STATUS']

            insert_query = f"""
            INSERT INTO {DELETE_JOB_CODE_AGG_TABLE} (JOB_CODE, DATE,ROW_COUNT_MSSQL, ROW_COUNT_SNOWFLAKE,AGG_ROW_COUNT,STATUS)
            VALUES ('{job_code}', '{date}', '{row_count_mssql}','{row_count_snowflake}','{agg_row_count}','{status_row_count}')
            """
            cursor.execute(insert_query)




# Define tasks using PythonOperator
process_tables_task = PythonOperator(
    task_id='process_tables_task',
    python_callable=process_tables,
    dag=dag
)

compare_and_delete_task = PythonOperator(
    task_id='compare_and_delete_task',
    python_callable=compare_and_delete,
    dag=dag
)

insert_task_second = PythonOperator(
    task_id='insert_diff_to_delete_job_code_table_2',
    python_callable=insert_diff_to_delete_job_code_table_2,
    provide_context=True,
    dag=dag
)



#####################################################################################################
def execute_send_second_email(**kwargs):
    # Define your queries
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
    today = datetime.now().strftime('%Y-%m-%d')
    table_query1 = """
        SELECT job_code, date, row_count_mssql,row_count_snowflake, agg_row_count FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE
        WHERE STATUS = '1차처리 필요';
    """
   
    table_query2 = """
        SELECT job_code, date, row_count_mssql,row_count_snowflake, agg_row_count FROM LOG_TABLE.DELETE_JOB_CODE_AGG_TABLE
        WHERE STATUS = '2차처리 완료' OR STATUS = '당일 데이터 미반영';

    """

    # Create Snowflake connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Execute first query
    cursor.execute(table_query1)
    columns1 = [col[0] for col in cursor.description]
    results1 = cursor.fetchall()
    df1 = pd.DataFrame(results1, columns=columns1)

    # Execute second query
    cursor.execute(table_query2)
    columns2 = [col[0] for col in cursor.description]
    results2 = cursor.fetchall()
    df2 = pd.DataFrame(results2, columns=columns2)

    cursor.close()
    conn.close()

    # Convert the two DataFrames to HTML separately
    email_content_1 = "<h3>1차 전체 값 검증:</h3>" + df1.to_html(index=False)
    email_content_2 = "<h3>DELETE 적용:</h3>" + df2.to_html(index=False)

    # Combine the HTML content for the email
    email_content = f"{email_content_1}<br><br>{email_content_2}"

    # Define the email task
    send_second_email_task = EmailOperator(
        task_id='send_email_2',
        to=['IVBI@eland.co.kr'],
        subject = f"DELETE_집계 테이블 현황_{datetime.now().strftime('%Y%m%d')}",
        html_content=email_content,
        dag=kwargs['dag'],
    )

    # Execute the email task
    send_second_email_task.execute(context=kwargs)

######################################################################




# Define the task to execute the view query, create DataFrame, and send email
execute_second_email_task = PythonOperator(
    task_id='execute_second_email',
    python_callable=execute_send_second_email,
    retries=9,
    provide_context=True,  # This is important to provide the context to the Python function
    dag=dag,
)



#############################################################






truncate_task >> truncate_task_agg_table >> insert_task >> insert_other_task >> execute_and_first_email_task >> process_tables_task >> compare_and_delete_task >> truncate_snowflake_task >> aggregate_task >> insert_task_second >> execute_second_email_task


