from airflow import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
import pendulum

SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_DATABASE = 'IVBI'
SCHEMA_NAME = 'ODS'
SOURCE_TABLE_NAME = 'TLS_USER'
SNOWFLAKE_WAREHOUSE = 'IVBI_DW'

def transfer_data_from_snowflake_to_mysql(**kwargs):
    # Snowflake connection
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    snowflake_conn = snowflake_hook.get_conn()
    snowflake_cursor = snowflake_conn.cursor()

    # MySQL connection
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()

    # Snowflake table schema
    snowflake_table = f'{SNOWFLAKE_DATABASE}.ODS.TLS_USER'
    snowflake_cursor.execute(f"DESCRIBE TABLE {snowflake_table}")
    table_schema = snowflake_cursor.fetchall()

    # MySQL table name
    mysql_table_test = 'TLS_USER_TEMP_ISV'
    mysql_cursor.execute(f"TRUNCATE {mysql_table_test}")
    mysql_table = 'TLS_USER_TEMP'
    mysql_cursor.execute(f"TRUNCATE {mysql_table}")
    
    # Fetch data from Snowflake
    snowflake_cursor.execute(f"SELECT * FROM {snowflake_table}")
    rows = snowflake_cursor.fetchall()
    column_names = [desc[0] for desc in snowflake_cursor.description]

    # Insert data into MySQL
    insert_test_query = f"INSERT INTO {mysql_table_test} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"
    mysql_cursor.executemany(insert_test_query, rows)
    insert_query = f"INSERT INTO {mysql_table} ({', '.join(column_names)}) VALUES ({', '.join(['%s'] * len(column_names))})"
    mysql_cursor.executemany(insert_query, rows)
    mysql_conn.commit()

    # Close connections
    snowflake_cursor.close()
    snowflake_conn.close()
    mysql_cursor.close()
    mysql_conn.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
}

dag = DAG(
    'BACKUP_AND_TRANSFER_TLS_USER_TEMP',
    default_args=default_args,
    description='A DAG to backup a Snowflake table within the same schema with a different table name',
    schedule_interval='0 1 * * 1-7',
    start_date=pendulum.datetime(2025, 3, 30, tz="Asia/Seoul"),
    catchup=False,
)



mig_mysql_table = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data_from_snowflake_to_mysql,
    dag=dag,
)


mig_mysql_table

