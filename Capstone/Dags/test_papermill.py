from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone


NOTEBOOKS_FOLDER = '/usr/local/airflow/dags/notebooks'

default_args = {
    'owner': 'zkan'
}
with DAG('test_papermill',
         schedule_interval='*/5 * * * *',
         default_args=default_args,
         start_date=timezone.datetime(2020, 8, 15),
         catchup=False) as dag:

    t1 = PapermillOperator(
        task_id='t1',
        input_nb=f'/opt/airflow/dags/datalake_etl_s3.ipynb',
        output_nb=f'/opt/airflow/dags/datalake_etl_s3_output.ipynb',
        # kernel_name='Python3',
    )

    t1

    # templated_command = """
    # papermill \
    # /usr/local/airflow/dags/notebooks/input.ipynb \
    # /usr/local/airflow/dags/notebooks/output.ipynb \
    # """

    # run_ntb = BashOperator(
    # task_id='run_ntb',
    # depends_on_past=False,
    # bash_command=templated_command,
    # dag=dag)

    # run_ntb
