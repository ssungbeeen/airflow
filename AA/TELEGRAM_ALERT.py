from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.models import DagRun, TaskInstance
from datetime import datetime, timedelta
import requests
from airflow.utils import timezone

def get_failed_dag_runs():

    now = timezone.utcnow()  
    one_hour_ago = now - timedelta(hours=2)


    with create_session() as session:
        failed_runs = (
            session.query(DagRun)
            .filter(
                DagRun.state == "failed",
                DagRun.execution_date >= one_hour_ago
            )
            .all()
        )

        results = []
        for dr in failed_runs:
            tasks_failed = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dr.dag_id,
                    TaskInstance.state == "failed",
                    TaskInstance.execution_date == dr.execution_date
                )
                .all()
            )
            for ti in tasks_failed:
                results.append({
                    "dag_id": dr.dag_id,
                    "task_id": ti.task_id,
                    "execution_date": dr.execution_date,
                    "start_date": ti.start_date,
                    "end_date": ti.end_date
                })
    return results




def send_telegram_message(message: str):
    token = "8274697519:AAEYHVpIBgp43OrUT43v4r2dZjZc0HMoST4"
    chat_id = "7220879074"
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    requests.post(url, data={"chat_id": chat_id, "text": message})


def notify_failed_dags():
    failed_tasks = get_failed_dag_runs()
    for ft in failed_tasks:
        msg = (
            f"⚠️ DAG 실패 알림\n"
            f"DAG: {ft['dag_id']}\n"
            f"Task: {ft['task_id']}\n"
            f"Execution: {ft['execution_date']}\n"
            f"Start: {ft['start_date']}\n"
            f"End: {ft['end_date']}"
        )
        send_telegram_message(msg)


dag = DAG(
    "FAILED_DAG_TELEGRAM_ALERT",
    schedule_interval=None,
    start_date=datetime(2025, 8, 19),
    catchup=False)


notify_failed_task = PythonOperator(
        task_id="notify_failed_dags",
        python_callable=notify_failed_dags,
        dag = dag
    )

notify_failed_task