from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

main_dag_id = 'scheduler_logs_cleanup'

args = {
    'owner': 'Airflow',
    'start_date': datetime(2021, 7, 21),
    'provide_context': True
}

dag = DAG(
    main_dag_id,
    catchup=False,
    concurrency=4,
    schedule_interval='@daily',
    default_args=args
)

# BashOperator를 사용하여 로그 파일 삭제 명령어 실행
clean_logs_command = """
    find /root/airflow/logs/scheduler -type d -mtime +2 -exec rm -rf {} +
"""

clean_logs_task = BashOperator(
    task_id='clean_logs',
    bash_command=clean_logs_command,
    dag=dag
)

# main DAG에 task 추가
clean_logs_task
