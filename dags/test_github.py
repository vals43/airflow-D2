from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dag_deploye_via_github',
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test', 'github_actions']
) as dag:

    test_task = BashOperator(
        task_id='validation_pipeline',
        bash_command='echo "Le déploiement automatique via GitHub Actions fonctionne !"'
    )