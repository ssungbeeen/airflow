from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import os
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect as snowflake_connector
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum

# 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1)
}

# DAG 생성
dag = DAG(
    'JOB_TABLE_INFO_UPLOAD_',
    default_args=default_args,
    description='Upload XLSX to Snowflake',
    start_date=pendulum.datetime(2024, 9, 20, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False
)

# 경로 설정
xlsx_file_path ='/root/airflow_bi/lib/python3.10/site-packages/airflow/example_dags/JOB_TABLE_INFO.xlsx'
table_name = 'JOB_TABLE_INFO'

def upload_xlsx_to_snowflake(**kwargs):
    # XLSX 파일 읽기
    df = pd.read_excel(xlsx_file_path, engine='openpyxl')
    df['UDATETIME'] = datetime.now().strftime('%Y-%m-%d')
    # Snowflake 연결
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default_2')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        # 테이블 초기화
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        # 데이터프레임을 Snowflake에 업로드
        success, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            table_name=table_name,
            quote_identifiers=False,
            use_logical_type=True,
            auto_create_table=False
        )

        if success:
            print(f"Successfully loaded {nrows} rows into {table_name}.")
        else:
            raise Exception(f"Failed to load data into {table_name}.")

    except Exception as e:
        error_message = str(e).replace("'", "''")
        print(f"Error: {error_message}")
        raise e

    finally:
        cursor.close()
        conn.close()

# PythonOperator 생성
upload_task = PythonOperator(
    task_id='upload_xlsx_to_snowflake',
    python_callable=upload_xlsx_to_snowflake,
    dag=dag
)

# DAG 설정
upload_task
