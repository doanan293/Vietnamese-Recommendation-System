from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

os.environ["PATH"] += os.pathsep + "/home/airflow/.wdm/drivers/edgedriver/linux64/125.0.2535.92"

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define the DAG
dag = DAG(
    'stock_analysis_pipeline',
    default_args=default_args,
    description='A simple stock analysis DAG',
    schedule_interval=timedelta(days=1),
)

# Define tasks using BashOperator
bctc_task = BashOperator(
    task_id='bctc_task',
    bash_command='python /opt/airflow/dags/CrawlBCTC.py',
    dag=dag,
)

crawl_news_task = BashOperator(
    task_id='crawl_news_task',
    bash_command='python /opt/airflow/dags/CrawlNews.py',
    dag=dag,
)

crawl_price_task = BashOperator(
    task_id='crawl_price_task',
    bash_command='python /opt/airflow/dags/CrawlPrice.py',
    dag=dag,
)

clustering_task = BashOperator(
    task_id='clustering_task',
    bash_command='python /opt/airflow/dags/Clustering.py',
    dag=dag,
)

chooseStock_task = BashOperator(
    task_id='chooseStock_task',
    bash_command='python /opt/airflow/dags/getinf.py',
    dag=dag,
)

# Define task dependencies
[crawl_news_task, crawl_price_task] >> bctc_task >> clustering_task >> chooseStock_task

if __name__ == "__main__":
    dag.cli()
