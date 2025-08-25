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
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
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
    'MONTHLY_AGG_',
    default_args=default_args,
    description='A simple data sync DAG',
    start_date=pendulum.datetime(2024, 12, 11, tz="Asia/Seoul"),
    schedule_interval='0 7 11 * *', 
    catchup=False,
)


update_check_date_sql = """
INSERT INTO BI_PRODCNT_DEDTL_SALESPART_M (CHECK_MONTH, GT_PRODUCT_DETAIL_DE, GT_PRODUCT_DETAIL_DE_NM,PROD_CNT) 
SELECT TO_CHAR(DATEADD(month, -1, CHECK_DATE),'yyyy-MM') AS CHECK_MONTH, GT_PRODUCT_DETAIL_DE, GT_PRODUCT_DETAIL_DE_NM, PROD_CNT
FROM BI_PRODCNT_DEDTL_SALESPART_1D;
"""


delete_old_records_sql = """

DELETE FROM BI_PRODCNT_DEDTL_SALESPART WHERE CAST(CHECK_DATE AS DATE) < CAST(DATEADD(day,-1,GETDATE()) AS DATE);
"""










load_data_task = SnowflakeOperator(
     task_id='load_data_to_target_table',
     snowflake_conn_id='snowflake_default_5',  
     sql=update_check_date_sql,
    )


delete_old_records_task = SnowflakeOperator(
    task_id='delete_old_records',
    snowflake_conn_id='snowflake_default_5',
    sql=delete_old_records_sql,
    dag=dag,
)



load_data_task >> delete_old_records_task 






        

