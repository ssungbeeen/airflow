from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import numpy as np
from pandas import NaT
from datetime import datetime, timedelta
import dateutil.relativedelta as rt
from airflow.operators.email_operator import EmailOperator
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
    'ISM--IVBI',
    default_args=default_args,
    description='ETL from MSSQL to Snowflake',
    start_date=pendulum.datetime(2025, 3, 20, tz="Asia/Seoul"),
    schedule_interval='30 5 * * MON-SUN',
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




def AGG_TABLE(**kwargs):

    today = datetime.today()

    START_DATE = ((today- relativedelta(days=1)).replace(day=1)).strftime('%Y-%m-%d')
    END_DATE = (today- relativedelta(days=1)).strftime('%Y-%m-%d')

    ISM_query = f"""EXEC [dbo].[EG00201_SELECT_PART_MINUS_DC] 1, '{START_DATE}','{END_DATE}', '', NULL, NULL, '047002', NULL, 'Y', 'N', '', '', '060003', '060001', '060002', '060004',  ''"""
    from_cursor.execute(ISM_query)
    ISM = from_cursor.fetchall()
    ISM = pd.DataFrame(ISM)
    ISM.columns = ['INDEX', '부서코드', '부서명', '매출액', '매출횟수','매출이익','NEGO','마진율','부서인원','수금액','상품코드수','거래처수']
    ISM = ISM.loc[ISM['부서코드'].isin([1,3,4,5,6,7,8,9,26,27,40]), ['부서코드','부서명','매출액','매출이익','거래처수']]
    
    
    IVBI_query = f"""
    SELECT CUST.PART_CODE, CUST.PART_NM
        , IFNULL(SUM(CAST(ORD.PRO_ORDER_AMOUNT AS FLOAT) - IFNULL(ORD.ORDER_UNIT_SALES_DC, 0)), 0)
                + -1 * SUM(CAST(IFNULL(ORD.RETURN_MONEY, 0) AS FLOAT) - IFNULL(ORD.DC_RETURN_AMOUNT, 0)) AS ORDER_AMOUNT
        , SUM((CAST(ORD.PRO_ORDER_AMOUNT AS FLOAT) - IFNULL(ORD.ORDER_UNIT_SALES_DC, 0)) - ORD.ORD_COST)
                 + -1 * SUM((CAST(ORD.RETURN_MONEY AS FLOAT) - IFNULL(ORD.DC_RETURN_AMOUNT, 0)) - ORD.VRY_COST) AS ORDER_MONEY --매출이익
        , COUNT(DISTINCT ORD.CUSTOMER_CODE) AS CUSTOMER_CNT --거래처수
    FROM MART.BI_ORDVRY_CUST_PAY ORD
    LEFT JOIN MART.MST_COURSEPART_CUST CUST ON ORD.CUSTOMER_CODE = CUST.CUSTOMER_CODE
    WHERE ORD.GT_PRODUCT_DETAIL_DE IN ('060003', '060001', '060002', '060004')
        AND CUST.PART_CODE IN (1,3,4,5,6,7,8,9,26,27,40)
        AND ORD.PAY_DAY BETWEEN '{START_DATE}' AND '{END_DATE}'
    GROUP BY CUST.PART_CODE, CUST.PART_NM
    ORDER BY PART_CODE ASC
    """
    
    
    
    to_cursor.execute(IVBI_query)
    IVBI = to_cursor.fetchall()

    IVBI = pd.DataFrame(IVBI)
    
    IVBI.columns = ['부서코드','부서명','매출액','매출이익','거래처수']
        

    RESULT = ISM.merge(IVBI, how='inner', on=['부서코드','부서명'], sort='부서코드', suffixes=('_ISM', '_IVBI'))     



    RESULT["매출액(ISM-IVBI)"] = RESULT["매출액_ISM"].astype(float) - RESULT["매출액_IVBI"].astype(float)
    RESULT["매출이익(ISM-IVBI)"] = RESULT["매출이익_ISM"].astype(float) - RESULT["매출이익_IVBI"].astype(float)
    RESULT["거래처수(ISM-IVBI)"] = RESULT["거래처수_ISM"] - RESULT["거래처수_IVBI"]


    RESULT["매출액 차이"] = np.where(abs(RESULT["매출액(ISM-IVBI)"]) > (RESULT["매출액_ISM"].astype(float) * 0.001),"Error","")
    RESULT["매출이익 차이"] = np.where(abs(RESULT["매출이익(ISM-IVBI)"]) > (RESULT["매출이익_IVBI"].astype(float) * 0.001),"Error","")
    RESULT["거래처수 차이"] = np.where(abs(RESULT["거래처수(ISM-IVBI)"]) > (RESULT["거래처수_IVBI"].astype(float) * 0.001),"Error","")

    float_col = ['매출액_ISM', '매출이익_ISM', '매출액_IVBI','매출이익_IVBI', '매출액(ISM-IVBI)', '매출이익(ISM-IVBI)']
    RESULT[float_col] = RESULT[float_col].astype(int)
    
    MESSAGE1 = RESULT[['부서코드','부서명','매출액(ISM-IVBI)','매출이익(ISM-IVBI)', '거래처수(ISM-IVBI)',"매출액 차이","매출이익 차이","거래처수 차이"]]

    MESSAGE2 = RESULT[['부서코드', '부서명', '매출액_ISM','매출액_IVBI', '매출이익_ISM','매출이익_IVBI', '거래처수_ISM','거래처수_IVBI']]
    line = f"""{START_DATE} ~ {END_DATE} 기간으로 조회한 ISM(부서별매출손익), IVBI(목표손익분석) 결과를 비교합니다\n(오차가 ISM값의 0.1% 이상일 때 Error 표시)"""


    to_cursor.close()
    from_cursor.close()
    from_conn.close()
    to_conn.close()

    STYLE = """
    <style>
        .dataframe { width: 100%; border-collapse: collapse; }
        .dataframe th { background-color: #f2f2f2; text-align: center; border: 1px solid black; padding: 8px; }
        .dataframe td { border: 1px solid black; padding: 8px; text-align: right; }
        .bold-red { font-weight: bold; color: red; }
    </style>
    """

# MESSAGE1 스타일링: '매출액 차이', '매출이익 차이', '거래처수 차이' 열 강조
    MESSAGE1_HTML = MESSAGE1.to_html(escape=False, index=False, border=1, justify='center')
    for col in ["매출액 차이", "매출이익 차이", "거래처수 차이"]:
        MESSAGE1_HTML = MESSAGE1_HTML.replace(f">{col}<", f" class='bold-red'>{col}<")
        for val in MESSAGE1[col]:
            MESSAGE1_HTML = MESSAGE1_HTML.replace(f">{val}<", f" class='bold-red'>{val}<")



    BODY = f"""
    {STYLE}
    <p>{START_DATE} ~ {END_DATE} 기간으로 조회한 ISM(부서별매출손익), IVBI(목표손익분석) 결과를 비교합니다<br>
    (오차가 ISM값의 0.1% 이상일 때 <span style='color: red; font-weight: bold;'>Error</span> 표시)</p>
    <h3>검증 결과</h3>
    {MESSAGE1_HTML}
    <h3>상세</h3>
    {MESSAGE2.to_html(escape=False, index=False, border=1, justify='center', classes='dataframe')}
    """

    # Define the email task
    send_email_task = EmailOperator(
        task_id='send_email',
        to=['IVBI@eland.co.kr'],
        subject=f"▶ISM IVBI 주요지표 값 검증_{datetime.now().strftime('%Y%m%d')}",       
        html_content=BODY,
        dag=kwargs['dag'],
    )

    # Execute the email task
    send_email_task.execute(context=kwargs)

execute_and_email_task = PythonOperator(
    task_id='execute_and_email',
    python_callable=AGG_TABLE,
    retries=9,
    provide_context=True,  # This is important to provide the context to the Python function
    dag=dag,
)
    

execute_and_email_task
    
    
