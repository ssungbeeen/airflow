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
    'BI_STOCKACCT_PROD_MART',
    default_args=default_args,
    description='ETL from MSSQL to Snowflake',
    start_date=pendulum.datetime(2024, 11, 16, tz="Asia/Seoul"),
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


    global from_conn, from_cursor, to_conn, to_cursor
from_conn, from_cursor = MSSQL_CONNECTOR()
to_conn, to_cursor = SNOWFLAKE_CONNECTOR()

def AGG_TABLE():

    today = datetime.today()

    START_DATE = datetime(today.year,7,1).strftime('%Y-%m-%d')
    END_DATE = datetime(today.year,7,31).strftime('%Y-%m-%d')
    ISM_query = f"""EXEC [WHE_GAE_JE_GO] 1,'{START_DATE}','{END_DATE}'"""
    from_cursor.execute(ISM_query)
    STOCKACCOUT = from_cursor.fetchall()
    STOCKACCOUT = pd.DataFrame(STOCKACCOUT)
    
    
    STOCKACCOUT.columns = ['상품코드', '품명 규격', '기초수량', '기초손실수량', '기초불량수량', 
    '기초_반품승인기준 차감수량', '기초_NO_VAT_수량', '기초합계', '시작이동평균가', 
    '기초금액', '기초 검증용', '★기초_반출대기 수량★', '매입량', '매입손실수량', 
    '매입불량수량', '매입_반품승인기준 차감수량', '매입수량합계', '평균매입가', 
    '손실제외_매입금액', '매입금액', '실매입', '매출수량', '매출불량수량', 
    '매출 NO VAT 수량', '일반_매출금액(VAT)', '일반_매출금액(NO VAT)', 
    '매출수량합계', '평균매출가', '평균매출가_VAT', '평균매출가_NO_VAT', 
    '매출금액', '매출반품차액', '불량제외 재고수량 합계(ISM)', '불량포함_재고수량 합계', 
    '불량제외 재고금액(ISM)', '불량포함 재고금액', '재고수량', '손실재고수량', 
    '재고_반품승인기준 차감수량', '재고_매출 NO VAT 수량', '이동평균가', 
    '재고수량 합계(ISM)', '재고금액(ISM)', '★반출대기 수량★', 
    '★재고수량 합계(ISM) + 반출대기 수량★', '재고수량 합계(ISM) + 반출대기 금액', 
    '주매입', '중분류', '대분류', '분류코드', '분류', '실재고(그룹) 검증용', '주매입처']
    
    
    zero_check_col = ['기초수량','기초손실수량','기초불량수량','기초_반품승인기준 차감수량','기초_NO_VAT_수량', '기초합계' 
                    ,'기초금액','★기초_반출대기 수량★','매입불량수량','매입_반품승인기준 차감수량','매입수량합계' 
                    , '실매입','매출불량수량','일반_매출금액(VAT)','매출수량합계','매출금액','★반출대기 수량★']

    # 해당 컬럼들이 모두 0인 행 삭제
    STOCKACCOUT = STOCKACCOUT.loc[~(STOCKACCOUT[zero_check_col] == 0).all(axis=1)]
    
    
    STOCKACCOUT.reset_index(drop=True, inplace=True)
    STOCKACCOUT['START_DATE'] = START_DATE
    STOCKACCOUT['END_DATE'] = END_DATE
    STOCKACCOUT['MIG_DATETIME'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    
    
    try :
        success, nchunks, nrows, _ = write_pandas(conn=to_conn
                                              ,df=STOCKACCOUT
                                              ,table_name= 'BI_STOCKACCT_PROD_1DD'
                                              ,database='IVBI'
                                              ,schema='MART'
                                              ,quote_identifiers=True
                                              ,auto_create_table=False
                                              ,use_logical_type=True)
        print("건수 : ", nrows)
        

        
        ##############################
        row_count_query = f"""
                        SELECT COUNT(*) 
                        FROM IVBI.MART.BI_STOCKACCT_PROD_1DD WHERE TO_CHAR(MIG_DATETIME,'YYYY-MM-DD') = TO_CHAR(CURRENT_TIMESTAMP,'YYYY-MM-DD')
                    """
        to_cursor.execute(row_count_query)
        row_count = to_cursor.fetchone()[0]  # 행 수를 가져옴
        start = datetime.now()
                    
        end = datetime.now()
        STATUS = 'O'
        ERROR = 'NULL'
        to_cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('BI_STOCKACCT_PROD_1DD','{start}', '{end}', '{STATUS}', '{row_count}','D','{ERROR}');''')
    except Exception as ERROR:
                    print('업로드 오류 발생 Error Message:', ERROR)
                    start = datetime.now()
                    STATUS = 'X'
                    to_cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('BI_STOCKACCT_PROD_1DD', '{start}', NULL, '{STATUS}','0','D','{ERROR}');''')


    to_cursor.close()
    from_conn.close()
    to_conn.close()








delete_old_records_sql_1D = """
DELETE FROM BI_STOCKACCT_PROD_1DD;
"""



update_mig_date_sql = """
INSERT INTO BI_STOCKACCT_PROD_MM
SELECT * FROM IVBI.MART.BI_STOCKACCT_PROD_1DD
"""




delete_old_records_sql_M = """
DELETE FROM BI_STOCKACCT_PROD_MM
WHERE MIG_DATETIME IN (
    SELECT MIG_DATETIME
    FROM (
        SELECT
            MIG_DATETIME,
            ROW_NUMBER() OVER (PARTITION BY "상품코드", "품명규격", START_DATE, END_DATE
                               ORDER BY MIG_DATETIME DESC) AS row_num
        FROM BI_STOCKACCT_PROD_MM
    ) AS SubQuery
    WHERE row_num > 1
);"""








truncate_task_1D = SnowflakeOperator(
    task_id='TRUNCATE_1DD',
    snowflake_conn_id='snowflake_default_5',
    sql=delete_old_records_sql_1D,
    dag=dag,
    )



AGG_TABLE = PythonOperator(
        task_id='AGG_TABLE',
        python_callable= AGG_TABLE,
        dag=dag
    )
    

load_data_task = SnowflakeOperator(
     task_id='LOAD_DATA_TO_TARGET_TABLE',
     snowflake_conn_id='snowflake_default_5',  
     sql=update_mig_date_sql,
     dag=dag,
    )
    
delete_old_records_task_M = SnowflakeOperator(
    task_id='DELETE_OLD_RECORDS_M',
    snowflake_conn_id='snowflake_default_5',
    sql=delete_old_records_sql_M,
    dag=dag,
    )    



truncate_task_1D >> AGG_TABLE >> load_data_task >> delete_old_records_task_M
    
    
