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
from airflow.operators.email_operator import EmailOperator
from airflow import DAG
import pandas as pd
from datetime import datetime
import pendulum
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0, 
    'execution_timeout': timedelta(minutes=60)
}

dag = DAG(
    'TEST_PIPELINE_ODS_TEST_REAL_TIME_HOURLY',
    default_args=default_args,
    description='A simple data sync DAG',
    start_date=pendulum.datetime(2024, 8, 23, tz="Asia/Seoul"),
    schedule_interval='0 8-19 * * MON-FRI',
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


def read_data(table_name, update_col_2):
    mssql_hook = MsSqlHook(mssql_conn_id='mssql_default')
    conn = mssql_hook.get_conn()
    cursor = conn.cursor(as_dict=True)  # 결과를 딕셔너리로 반환
    read_table_query = f"SELECT * FROM ISM.dbo.{table_name} WITH(NOLOCK) WHERE {update_col_2}"
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




def insert_new_data(each_table, DATA, each_job, update_col):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
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
                        add_column_query = f"ALTER TABLE IVBI.ODS.{each_table} ADD COLUMN {column_name} VARCHAR"
                        cursor.execute(add_column_query)                        

                        print(f"Added column {column_name} to table {each_table}")

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
                
job_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_INFO WHERE AGG_LEVEL = '0' AND DEL_YN != 'Y' AND SCHEDULE = 'H' AND (JOB_CODE !='TLS_COURSE') AND (JOB_CODE!='TLS_ORDER_VARY_DC') AND (JOB_CODE !='TLS_ORDER_VARY')"
update_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_TABLE_INFO WHERE DEL_YN != 'Y' AND UPDATE_COL_RT != 'X'"

def process_jobs(**context):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    conn = snowflake_hook.get_conn()
    cursor = None
    try:
        cursor = conn.cursor()
        job_info = pd.read_sql(job_info_query, conn)
        ods_job_list = job_info['JOB_CODE'].tolist()
        update_info = pd.read_sql(update_info_query, conn)

        for each_job in ods_job_list:
            try:
                each_update_col = update_info[update_info['JOB_CODE'] == each_job]['UPDATE_COL_RT']
                each_update_col_2 = update_info[update_info['JOB_CODE'] == each_job]['UPDATE_COL_RT_2']
                if len(each_update_col) == 1:
                    update_col = each_update_col.iloc[0]  # UPDATE_COL 값을 추출
                    update_col_2 = each_update_col_2.iloc[0]
                    DATA = read_data(each_job, update_col_2)
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




def fetch_data(query, conn_id):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)  
    connection = hook.get_conn()  
    cursor = connection.cursor()  
    cursor.execute(query)  
    data = cursor.fetchall()  
    column_names = [desc[0] for desc in cursor.description]  
    df = pd.DataFrame(data, columns=column_names)  
    cursor.close()  
    connection.close()  
    return df  

def process_mart_jobs(**kwargs):
    conn_id = 'snowflake_default_2'
    today = datetime.now().strftime('%Y-%m-%d')
    hook = SnowflakeHook(snowflake_conn_id=conn_id)  
    connection = hook.get_conn()  
    cursor = connection.cursor()  
    today = datetime.now().strftime('%Y-%m-%d')
    hour = datetime.now().hour
    # JOB 리스트 테이블
    job_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_INFO WHERE DEL_YN != 'Y' AND SCHEDULE = 'H' AND (JOB_CODE != 'BI_ORDER_PROD' AND JOB_CODE != 'BI_VARY_PROD' AND JOB_CODE != 'BI_ORDVRY_PROD_CALL' AND JOB_CODE != 'BI_ORDVRY_PROD_PAY') AND AGG_LEVEL >= 1 ORDER BY AGG_LEVEL ASC"
    JOB_INFO = fetch_data(job_info_query, conn_id)
    
    # JOB별 선행 JOB 테이블
    job_mart_info_query = "SELECT * FROM IVBI.LOG_TABLE.PRE_JOB_INFO WHERE DEL_YN != 'Y' AND SCHEDULE = 'H'"
    JOB_MART_INFO = fetch_data(job_mart_info_query, conn_id)
    
    JOB_LIST = JOB_INFO['JOB_CODE'].tolist()
    

    
    for each_job in JOB_LIST:
        cursor.execute("ALTER SESSION SET TIMEZONE = 'Asia/Seoul'")
        cursor.execute("SELECT CURRENT_TIMESTAMP;")
        today = datetime.now().strftime('%Y-%m-%d')
        job_log_query = f'''
        SELECT * FROM IVBI.LOG_TABLE.JOB_LOG_MART
        WHERE TO_CHAR(START_DATE,'YYYY-MM-DD') = '{today}'
        AND HOUR(START_DATE) = {hour}
        AND SCHEDULE = 'H' AND STATUS='O'
        '''
        JOB_LOG = fetch_data(job_log_query, conn_id)   
    
        
        if len(JOB_LOG.loc[JOB_LOG['JOB_CODE'] == each_job])==0:
            success_prejob_cnt=0
            PRE_JOB_LIST = JOB_MART_INFO.loc[(JOB_MART_INFO['JOB_CODE'] == each_job) ,'PRE_JOB_CODE'].to_list()
            print("선행작업 : ", PRE_JOB_LIST)
            for pre_each_job in PRE_JOB_LIST: #선행잡 체크
                if len(JOB_LOG.loc[JOB_LOG['JOB_CODE'] == pre_each_job]) >=1:
                    success_prejob_cnt = success_prejob_cnt+1

  


        
            if len(PRE_JOB_LIST) == success_prejob_cnt: # 모든 선행잡이 성공됐으면


                try:
                    print(each_job, '실행중')
                    start = datetime.now()
                    cursor.execute(f'''TRUNCATE TABLE IVBI.MART.{each_job}''')
                    cursor.execute(f'''INSERT INTO IVBI.MART.{each_job} SELECT * FROM IVBI.MART.V_{each_job};''')
                    # ROW_COUNT 계산
                    row_count_query = f"""
                        SELECT COUNT(*) 
                        FROM IVBI.MART.{each_job};
                    """
 
                    cursor.execute(row_count_query)
                    row_count = cursor.fetchone()[0]  # 행 수를 가져옴

                    end = datetime.now()
                    STATUS = 'O'
                    ERROR = 'NULL'
                    cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}','{start}', '{end}', '{STATUS}', '{row_count}','H','{ERROR}');''')
                except Exception as ERROR:
                    print('업로드 오류 발생 Error Message:', ERROR)
                    STATUS = 'X'
                    cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', NULL, NULL, '{STATUS}','0','H','{ERROR}');''')
            else:
                print("선행 작업에 오류가 있습니다")
                STATUS = 'X'
                ERROR = 'PRE JOB 오류가 있습니다'
                cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', NULL, NULL, 'X','0','H','PRE JOB 오류가 있습니다');''')
        else:
            print("이미 완료된 작업입니다.")
            cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', NULL, NULL, 'X','0','H','이전에 작업 완료되었습니다');''')
        





    connection.commit()
    cursor.close()
    connection.close()




    
process_jobs_mart_task = PythonOperator(
        task_id='process_mart_jobs',
        python_callable=process_mart_jobs,
        dag=dag,
    )





def execute_view_query_and_send_email(**kwargs):
    # Define your queries
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
    today = datetime.now().strftime('%Y-%m-%d')

    table_query1 = """
    SELECT 
    ROW_NUMBER() OVER (ORDER BY INSERT_TIME ASC) AS NUM,
    job_code, 
    date, 
    insert_time, 
    row_count,
    status
    FROM 
    LOG_TABLE.JOB_TABLE_LOG
    WHERE 
    TO_CHAR(INSERT_TIME, 'YYYY-MM-DD') = (
        SELECT TO_CHAR(MAX(INSERT_TIME), 'YYYY-MM-DD')
        FROM LOG_TABLE.JOB_TABLE_LOG
    ) AND EXTRACT(HOUR FROM INSERT_TIME) = EXTRACT(HOUR FROM (SELECT MAX(INSERT_TIME) FROM LOG_TABLE.JOB_TABLE_LOG))
    AND SCHEDULE = 'H';
    """
   
    table_query2 = """
        SELECT ROW_NUMBER() OVER (ORDER BY START_DATE ASC) AS NUM, job_code, start_date, end_date, row_count, status FROM IVBI.LOG_TABLE.JOB_LOG_MART 
        WHERE TO_CHAR(START_DATE,'YYYY-MM-DD') = (SELECT TO_CHAR(MAX(START_DATE),'YYYY-MM-DD')
        FROM LOG_TABLE.JOB_LOG_MART)
        AND EXTRACT(HOUR FROM START_DATE) = EXTRACT(HOUR FROM (SELECT MAX(START_DATE) FROM LOG_TABLE.JOB_LOG_MART))
        AND SCHEDULE = 'H' AND STATUS='O';

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
    email_content_1 = "<h3>ODS Results:</h3>" + df1.to_html(index=False)
    email_content_2 = "<h3>MART Results:</h3>" + df2.to_html(index=False)

    # Combine the HTML content for the email
    email_content = f"{email_content_1}<br><br>{email_content_2}"

    # Define the email task
    send_email_task = EmailOperator(
        task_id='send_email',
        to=['IVBI@eland.co.kr'],
        subject=f"SNOWFLAKE 실시간 집계 현황_{datetime.now().strftime('%H시 기준')}",       
        html_content=email_content,
        dag=kwargs['dag'],
    )

    # Execute the email task
    send_email_task.execute(context=kwargs)

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1)
}

# Instantiate a DAG


# Define the task to execute the view query, create DataFrame, and send email
execute_and_email_task = PythonOperator(
    task_id='execute_and_email',
    python_callable=execute_view_query_and_send_email,
    retries=9,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # This is important to provide the context to the Python function
    dag=dag,
)

# Set task dependencies
process_jobs_task >>process_jobs_mart_task >> execute_and_email_task


