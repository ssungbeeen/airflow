from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow import DAG
import pandas as pd
from datetime import datetime
import pendulum

# Define the function to execute the queries and send the email
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
        to=['LIM_SUNGBEEN@eland.co.kr'],
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


# Instantiate a DAG
dag = DAG(
    'EMAIL_REAL_TIME',
    default_args=default_args,
    description='A DAG to execute Snowflake queries, convert results to separate DataFrames, and send via email',
    start_date=pendulum.datetime(2024, 8, 23, tz="Asia/Seoul"),
    schedule_interval=None,
    catchup=False
)

# Define the task to execute the view query, create DataFrame, and send email
execute_and_email_task = PythonOperator(
    task_id='execute_and_email',
    python_callable=execute_view_query_and_send_email,
    retries=9,
    provide_context=True,  # This is important to provide the context to the Python function
    dag=dag,
)

# Set task dependencies
execute_and_email_task
