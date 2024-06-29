from datetime import timedelta,datetime
from pathlib import Path
from dotenv import load_dotenv
from airflow import DAG
from sqlalchemy import create_engine

import json
import requests
import psycopg2
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os
import hashlib
import logging
from psycopg2.extras import execute_values


load_dotenv()  # take environment variables from .env.

dag_path = os.getcwd() 

### Descarga base de Api ###

def extraer_data():
    token:str = "BEARER eyJhbGciOiJIUzUxMiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3NDg3Mjg4NDEsInR5cGUiOiJleHRlcm5hbCIsInVzZXIiOiJtYXJjb3MuY2VydmVyYUBncnVwb3NhbmNyaXN0b2JhbC5jb20ifQ.7BIMFrn8dExn-Vyq8KS275NXlpn3mtOnxWnZowEGrPjBN1b-aYfgW1baMV_-1q0pLmuTmG7K4kPqHnXcZvYdZg"
    endpoints = ['milestones','usd','usd_of','usd_of_minorista','base','reservas','circulacion_monetaria',
             'depositos','cuentas_corrientes','cajas_ahorro','plazo_fijo','cer','uva','inflacion_mensual_oficial',
             'inflacion_interanual_oficial']
    
    df = pd.DataFrame(columns=['fecha', 'valor', 'variable'])
    headers = {"Authorization": token}

    for endpoint in endpoints:
        url = "https://api.estadisticasbcra.com/" + endpoint
        response = requests.get(url, headers=headers)
        
        # Comprobar si la solicitud fue exitosa
        if response.status_code == 200:
            data_json = response.json()
            data = pd.DataFrame(data_json)
            data = data.rename(columns={'d': 'fecha', 'v': 'valor'})
            data['variable'] = endpoint
            df = pd.concat([df, data], ignore_index=True)
        else:
            # Si hubo un error en la solicitud, imprimir el código de estado
            print(f"Error {response.status_code} al obtener datos del endpoint: {endpoint}")
    
    df.to_csv(os.path.join(dag_path, 'raw_data' ,'data_api.csv'), index = False)
    

### Transforma data ###

def anonymize(value):
    if pd.isnull(value):
        return value
    if isinstance(value, (int, float)):
        value = str(value)
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def transformar_data():
    df = pd.read_csv(os.path.join(dag_path, 'raw_data' ,'data_api.csv'))
    df = df[df["variable"] != 'milestones']
 #   df = df.drop(['e', 't'], axis=1)
    df = df.pivot(index='fecha', columns='variable', values='valor')
    df = df.reset_index()
    df['fecha'] = pd.to_datetime(df['fecha'])
    df2 = df[df["fecha"] >= '2024-01-01']

    cols_anonimizar = ["usd","usd_of","usd_of_minorista"]

    for col in cols_anonimizar:
        df2[f"{col}_hash"] = df2[col].apply(anonymize)
                     
    df2.to_csv(os.path.join(dag_path, 'processed_data' ,'data_transform.csv'), index = False)                     


### Conexión con Redshift ###

url = os.getenv("REDSHIFT_URL")
user = os.getenv("REDSHIFT_USER")
pwd = os.getenv("REDSHIFT_PWD")
data_base = os.getenv("REDSHIFT_DB")

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

def conexion_redshift():
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Connected to Redshift successfully!")
    except Exception as e:
        print("Unable to connect to Redshift.")
        print(e)
    with conn.cursor() as cur:
        cur.execute("Truncate table stage_bcra_hash")
        count = cur.rowcount
        conn.close()     


### Cargar datos ###

def cargar_data():    
    df = pd.read_csv(os.path.join(dag_path, 'processed_data' ,'data_transform.csv'))
    # conexion a database
    url=os.getenv("REDSHIFT_URL")
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')
    
    # Definir columnas
    columns= ['fecha',	'base',	'cajas_ahorro',	'cer',	'circulacion_monetaria',	
              'cuentas_corrientes',	'depositos',	'inflacion_interanual_oficial',	
              'inflacion_mensual_oficial',	'plazo_fijo',	'reservas',	'usd',	'usd_of',	
              'usd_of_minorista',	'uva',	'usd_hash',	'usd_of_hash',	'usd_of_minorista_hash']
    cur = conn.cursor()
    # Define the table name
    table_name = 'stage_bcra_hash'
    # Define the columns you want to insert data into
    columns = columns
    # Generate 
    values = [tuple(x) for x in df.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    # Execute the INSERT statement using execute_values
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")    
    #df.to_sql('mining_data', engine, index=False, if_exists='append')  


# argumentos por defecto para el DAG
default_args = {
    'owner': 'marcos',
    'start_date': datetime(2024,6,29),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

BC_dag = DAG(
    dag_id='etl_bcra',
    default_args=default_args,
    description='Agrega data de forma diaria',
    schedule_interval="@daily",
    catchup=False
)


### Tareas ###
#1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=BC_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32    