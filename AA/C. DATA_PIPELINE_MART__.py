from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
import pandas as pd
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1),
    'retries': 0,
    'provide_context': False,
}



dag = DAG(
    'C.SNOWFLAKE_PIPELINE_MART',
    default_args=default_args,
    description='DATA_PIPELINE_MART DAG',
    start_date=pendulum.datetime(2024, 10, 9, tz="Asia/Seoul"),    
    schedule_interval='15 5 * * 1-7',
    catchup=False,
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

def process_jobs(**kwargs):
    conn_id = 'snowflake_default_2'
    today = datetime.now().strftime('%Y-%m-%d')
    hook = SnowflakeHook(snowflake_conn_id=conn_id)  
    connection = hook.get_conn()  
    cursor = connection.cursor()  
    today = datetime.now().strftime('%Y-%m-%d')
    status_check_query = f"""
        SELECT * FROM IVBI.LOG_TABLE.JOB_TABLE_LOG
        WHERE TO_CHAR(INSERT_TIME, 'YYYY-MM-DD') = '{today}' AND (STATUS = 'X' OR STATUS IS NULL)
    """
    STATUS_LOG = fetch_data(status_check_query, conn_id)
    
    if not STATUS_LOG.empty:
        print("오늘 날짜에 STATUS가 'X'로 기록된 작업이 있어 프로세스를 중단합니다.")
        return    
            

    # JOB 리스트 테이블
    job_info_query = "SELECT * FROM IVBI.LOG_TABLE.JOB_INFO WHERE DEL_YN != 'Y' AND SCHEDULE = 'D' AND AGG_LEVEL >= 1 AND JOB_CODE != 'BI_STOCKACCT_PROD_1D' ORDER BY AGG_LEVEL ASC"
    JOB_INFO = fetch_data(job_info_query, conn_id)
    
    # JOB별 선행 JOB 테이블
    job_mart_info_query = "SELECT * FROM IVBI.LOG_TABLE.PRE_JOB_INFO WHERE DEL_YN != 'Y' AND SCHEDULE = 'D'"
    JOB_MART_INFO = fetch_data(job_mart_info_query, conn_id)
    
    JOB_LIST = JOB_INFO['JOB_CODE'].tolist()
    

    
    for each_job in JOB_LIST:
        job_log_query = f"""
            SELECT * FROM IVBI.LOG_TABLE.JOB_LOG_MART
            WHERE TO_CHAR(START_DATE, 'YYYY-MM-DD') = '{today}' AND STATUS = 'O'
        """
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
                    if each_job == 'BI_UNPAID_CUST':
                        print(each_job, '은(는) 건너뜀')
                        continue         
                               
                    print(each_job, '실행중')
                    start = datetime.now()
                    cursor.execute(f"SELECT ACCUMULATE FROM JOB_INFO WHERE JOB_CODE = '{each_job}'")
                    accumulate_flag = cursor.fetchone()[0]
                    if accumulate_flag == 'O':
                        cursor.execute(f'''DROP TABLE IF EXISTS IVBI.MART.{each_job};''')
                        cursor.execute(f'''CREATE TABLE IVBI.MART.{each_job} AS SELECT * FROM IVBI.MART.V_{each_job};''')
                        insert_table = each_job[:-3] if each_job.endswith('_1D') else each_job
                        cursor.execute(f'''INSERT INTO IVBI.MART.{insert_table} SELECT * FROM IVBI.MART.V_{each_job};''')
                    elif accumulate_flag == 'OO':
                        insert_table = each_job[:-3] + '_M' if each_job.endswith('_1D') else each_job
                        cursor.execute(f'''DELETE FROM IVBI.MART.{insert_table} WHERE CAST(ADATETIME AS DATE) = DATEADD(DAY, -1, CURRENT_DATE) AND SUBSTR(CHECK_MONTH, 1, 7) = TO_CHAR(DATEADD(MONTH, 0, CURRENT_DATE),'YYYY-MM');''')                    
                        cursor.execute(f'''DROP TABLE IF EXISTS IVBI.MART.{each_job};''')
                        cursor.execute(f'''CREATE TABLE IVBI.MART.{each_job} AS SELECT * FROM IVBI.MART.V_{each_job};''')

                        cursor.execute(f'''INSERT INTO IVBI.MART.{insert_table} SELECT * FROM IVBI.MART.V_{each_job};''')
                    else:
                        
                        cursor.execute(f'''DROP TABLE IF EXISTS IVBI.MART.{each_job};''')
                        cursor.execute(f'''CREATE TABLE IVBI.MART.{each_job} AS SELECT * FROM IVBI.MART.V_{each_job};''')                      
                    
                    

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
                    cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}','{start}', '{end}', '{STATUS}', '{row_count}','D','{ERROR}');''')
                except Exception as ERROR:
                    print('업로드 오류 발생 Error Message:', ERROR)
                    start = datetime.now()
                    STATUS = 'X'
                    cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', '{start}', NULL, '{STATUS}','0','D','{ERROR}');''')
            else:
                print("선행 작업에 오류가 있습니다")
                start = datetime.now()
                STATUS = 'X'
                ERROR = 'PRE JOB 오류가 있습니다'
                cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', '{start}', NULL, 'X','0','D','PRE JOB 오류가 있습니다');''')
        else:
            print("이미 완료된 작업입니다.")
            cursor.execute(f'''INSERT INTO IVBI.LOG_TABLE.JOB_LOG_MART VALUES ('{each_job}', NULL, NULL, 'X','0','D','이전에 작업 완료되었습니다');''')
        





    connection.commit()
    cursor.close()
    connection.close()




    
process_jobs_task = PythonOperator(
        task_id='process_jobs',
        python_callable=process_jobs,
        dag=dag,
    )






# Set task dependencies
process_jobs_task 

