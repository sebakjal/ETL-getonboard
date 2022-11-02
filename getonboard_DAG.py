from __future__ import annotations
import datetime
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Seteo de variables
workingpath = '/home/kjal/jobs_csv/'
today = datetime.date.today().strftime("%Y-%m-%d")

# Cambio al directorio de trabajo o creación de este si no existe
if os.path.isdir(workingpath):
    os.chdir(workingpath)
else:
    os.mkdir(workingpath)
    os.chdir(workingpath)

# DAG principal
# La primera task es un comando bash que corre el script Python para extraer la información de la página getonbrd.cl
# La segunda task espera 10 segundos para asegurarse que el archivo resultante del script anterior se cree correctamente
# La tercera task carga los datos del csv del día de hoy (creado en la primera task) a la tabla del proyecto en BigQuery


# Argumentos para enviar correo en caso de fallo
default_args = {
    'owner': 'Kjal',
    'email': ['seba.carvajal.7@gmail.com'],
    'email_on_failure': True}

with DAG(
        dag_id='getonboard_dag',
        default_args=default_args,
        schedule='50 2 * * *',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['proyecto1']
) as dag:
    run_this = BashOperator(
        task_id='run_this',
        bash_command='python3 /home/kjal/airflow/dags/import_requests.py',
    )

    sleep_ten = BashOperator(
        task_id='sleep_ten',
        bash_command='sleep 10'
    )

    run_this_last = BashOperator(
        task_id='run_this_last',
        bash_command='bq load --skip_leading_rows=1 dataset.jobs {}{}_jobs.csv'.format(workingpath, today),
    )

    run_this >> sleep_ten >> run_this_last

if __name__ == "__main__":
    dag.cli()
