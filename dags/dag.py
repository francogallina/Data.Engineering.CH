from datetime import timedelta, datetime
import json
from airflow import DAG
import funciones
from airflow.operators.python_operator import PythonOperator
import os


# Argumentos por defecto para el DAG
default_args = {
    'start_date': datetime(2023,5,30),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='Precio_acciones_Dow_Jones',
    default_args=default_args,
    description='Extraer diariamente el valor de las acciones dentro del indice Dow Jones',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     #Path original.. home en Docker

# Obtener la API KEY de un archivo externo
with open(dag_path+'/dags/'+"config.json") as config_file:
  config = json.load(config_file)

api_key_twelvedata = config.get('api_key_twelvedata')
drivername_db = config.get('drivername_db')
host_db = config.get('host_db')
database_db = config.get('database_db')
username_db = config.get('username_db')
password_db = config.get('password_db')
    

#1. Extraccion datos de la API
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=funciones.get_stock_data,
    dag=dag,
)

# 2. Envio de data 
task_2= PythonOperator(
    task_id="conexion_BD",
    python_callable=funciones.conexion_redshift,
    dag=dag
)

# 3. Envio de data 
task_3= PythonOperator(
    task_id="envío_email",
    python_callable=funciones.send_email,
    dag=dag
)

task_1 >> task_2 >> task_3