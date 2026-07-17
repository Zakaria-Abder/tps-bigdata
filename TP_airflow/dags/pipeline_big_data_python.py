import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def ingestion():
    print("Ingestion des données")


def nettoyage():
    print("Nettoyage des données")


def analyse():
    print("Analyse des données")


def rapport():
    print("Génération du rapport")


with DAG(
    dag_id="pipeline_big_data_python",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["python"],
) as dag:

    t1 = PythonOperator(
        task_id="ingestion",
        python_callable=ingestion,
    )

    t2 = PythonOperator(
        task_id="nettoyage",
        python_callable=nettoyage,
    )

    t3 = PythonOperator(
        task_id="analyse",
        python_callable=analyse,
    )

    t4 = PythonOperator(
        task_id="rapport",
        python_callable=rapport,
    )

    t1 >> t2 >> t3 >> t4