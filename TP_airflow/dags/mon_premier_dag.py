import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def bonjour():
    print("Bonjour Airflow !")


with DAG(
    dag_id="mon_premier_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:

    tache1 = PythonOperator(
        task_id="bonjour",
        python_callable=bonjour,
    )