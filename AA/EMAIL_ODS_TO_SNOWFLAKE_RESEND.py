from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
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
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime, time, timedelta
import pendulum
import re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import pendulum




def execute_view_query_and_send_email(**kwargs):
    # Connect to Snowflake using Airflow's SnowflakeHook
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
    
    # Execute the query
    query = """SELECT * FROM IVBI.LOG_TABLE.V_CHECK_ODS_D"""
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    
    # Fetch and process the results
    results = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(results, columns=columns)
    
    # Convert 'START TIME' to time and calculate run time
    df['START_TIME'] = pd.to_datetime(df['START_TIME'], format='%H:%M:%S').dt.time
    ods_total_run_time = datetime.combine(datetime.min, max(df['START_TIME'])) - datetime.combine(datetime.min, min(df['START_TIME']))
    
    # Calculate success and total count
    success_count = len(df[df['STATUS'] == 'O'])
    total_count = len(df)
    
    # Create the email message
    message = f"""
    <h3>★IVBI ODS 마이그 모니터링★</h3>
    <p><b>성공:</b> {success_count} / <b>전체:</b> {total_count}</p>
    <p><b>소요시간:</b> {ods_total_run_time}</p>
    <h4>Details:</h4>
    {df.to_html(index=False)}
    """
    
    # Send email using EmailOperator
    email_operator = EmailOperator(
        task_id='send_email',
        to='LIM_SUNGBEEN@eland.co.kr',
        subject=f"★IVBI ODS 마이그 모니터링★_{datetime.now().strftime('%Y%m%d')}",
        html_content=message,
    )
    email_operator.execute(context=kwargs)
    
    
default_args = {
    'owner': 'your_name',

    'start_date': datetime(2024, 8, 1)
}

# Instantiate a DAG
dag = DAG(
    'EMAIL_ODS_RESEND',
    default_args=default_args,
    description='A DAG to execute a PostgreSQL view query, convert results to DataFrame, and send via email',
    start_date=pendulum.datetime(2024, 10, 13, tz="Asia/Seoul"),
    schedule_interval=None,  
    catchup=False)

# Define the task to execute the view query, create DataFrame, and send email
execute_and_email_task = PythonOperator(
    task_id='execute_and_email',
    python_callable=execute_view_query_and_send_email,
    retries = 9,
    provide_context=True,  # This is important to provide the context to the Python function
    dag=dag,
)



# Set task dependencies
execute_and_email_task 

