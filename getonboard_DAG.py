import datetime
import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# Seteo de variables
working_path = '/home/kjal/jobs_csv/'
owner = 'Kjal_notebook'
emails_alerting = ['seba.carvajal.7@gmail.com']
script_path = '/home/kjal/airflow/dags/web_request.py'
bq_table = 'dataset.jobs'
gcs_backup_bucket = 'getonbrd_csv_backup'
today = datetime.date.today().strftime('%Y-%m-%d')

# Cambio al directorio de trabajo o creación de este si no existe
if os.path.isdir(working_path):
    os.chdir(working_path)
else:
    os.mkdir(working_path)
    os.chdir(working_path)

# DAG principal
# La primera task es un comando bash que corre el script Python para extraer la información de la página getonbrd.cl
# La segunda task espera 10 segundos para asegurarse que el archivo resultante del script anterior se cree correctamente
# La tercera task carga los datos del csv del día de hoy (creado en la primera task) a la tabla del proyecto en BigQuery
# Y también sube el archivo generado hacia Cloud Storage a modo de respaldo

# Argumentos para enviar correo en caso de fallo
default_args = {
    'owner': owner,
    'email': emails_alerting,
    'email_on_failure': True}

with DAG(
        dag_id='getonbrd_dag',
        default_args=default_args,
        schedule='50 2 * * *',  # equivalente a las 23:50 hora Chile continental
        start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=['proyecto_getonbrd']
) as dag:
    web_request = BashOperator(
        task_id='web_request',
        bash_command='python3 {}'.format(script_path)
    )

    sleep_ten = BashOperator(
        task_id='sleep_ten',
        bash_command='sleep 10'
    )

    bq_load = BashOperator(
        task_id='bq_load',
        bash_command='bq load --skip_leading_rows=1 {} {}{}_jobs.csv'.format(bq_table, working_path, today)
    )

    gcs_backup = BashOperator(
        task_id='gcs_backup',
        bash_command='gsutil cp -n {}*.csv gs://{}'.format(working_path, gcs_backup_bucket)
    )

    web_request >> sleep_ten >> [bq_load, gcs_backup]

if __name__ == "__main__":
    dag.cli()
