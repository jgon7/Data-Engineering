from datetime import timedelta,datetime
from pathlib import Path
import json
import requests
import psycopg2
from airflow import DAG
from sqlalchemy import create_engine
# Operadores
from airflow.operators.python_operator import PythonOperator
#from airflow.utils.dates import days_ago
import pandas as pd
import os

dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'JessicaGonzalez',
    'start_date': datetime(2023,12,4),
    'retries':0,
    'retry_delay': timedelta(minutes=5)
}

MARVEL_dag = DAG(
    dag_id='ETL_Marvel',
    default_args=default_args,
    description='Información de Personajes de Marvel',
    schedule_interval="@daily",
    catchup=False
)

dag_path = os.getcwd()     

# funcion de extraccion de datos
def extraer_data(exec_date):
    try:
         print(f"Adquiriendo data para la fecha: {exec_date}")
         date = datetime.strptime(exec_date, '%Y-%m-%d %H')
         url = "http://gateway.marvel.com/v1/public/characters?ts=1&apikey=7e3a2d5eac655fa924616c54f196c38a&hash=96471a08b6305de87d0497523a8e92a5"
         headers = {"Accept-Encoding": "gzip, deflate"}
         response = requests.get(url, headers=headers)
         if response:
              print('Exito')
              data = response.json()
              with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(data, json_file)
         else:
              print('Error') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       

# Funcion de transformacion en tabla
def transformar_data(exec_date):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
        loaded_data=json.load(json_file)
        
    resul =  loaded_data['data']['results']
    i = 0
    listId = []
    listName = []
    listDescription = []
    listCantComics = []
    listCantSeries = []
    listCantEventos = []
    listStories = []

    for m in resul:
        auxResul = (resul[i])
        listId.append(auxResul['id'])
        listName.append(auxResul['name'])
        listDescription.append(auxResul['description'])
        listCantComics.append(auxResul['comics']['returned'])
        listCantSeries.append(auxResul['series']['returned'])
        listCantEventos.append(auxResul['events']['returned'])
        listStories.append(auxResul['stories']['returned'])
        i = i+1

    d = {'Nombre': listName, 'Descripcion' : listDescription, 'Comics': listCantComics, 'Series': listCantSeries, 'Historias': listStories, 'Eventos': listCantEventos }
    df = pd.DataFrame(data=d, index=listId)
    df.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')

# Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=redshift_conn["database"],
            user=redshift_conn["username"],
            password=redshift_conn["pwd"],
            port='5439')
        print(conn)
        print("Conectado a RedShift con éxito")
    except Exception as e:
        print("No se pudo conectar a RedShift" + e)

from psycopg2.extras import execute_values

# Funcion de envio de info Marvel
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    print(records.shape)
    print(records.head())

    # conexion a bd
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    conn = psycopg2.connect(
        host=url,
        dbname=redshift_conn["database"],
        user=redshift_conn["username"],
        password=redshift_conn["pwd"],
        port='5439')

    # cargar info en tabla en redshift
    dtypes= records.dtypes
    cols= list(dtypes.index )
    cols.remove("Nombre")
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','int32': 'INT','float64': 'FLOAT','object': 'VARCHAR(500)','bool':'BOOLEAN'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    table_schema = f"""
            CREATE TABLE IF NOT EXISTS marvel_info_personajes (
                Nombre VARCHAR(500) PRIMARY KEY,
                {', '.join(column_defs)}
            );
            TRUNCATE TABLE marvel_info_personajes;
            """
           

    cur = conn.cursor()
    cur.execute(table_schema)
    table_name = 'marvel_info_personajes'
    columns = ['Nombre' , 'Descripcion' , 'Comics' , 'Series' , 'Historias' , 'Eventos']
    values = [tuple(x) for x in records.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    

# Tareas

#1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_info',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=MARVEL_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_info',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=MARVEL_dag,
)

# 3. Envio de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_DB",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=MARVEL_dag
)

# 3.2 Envio final
task_32 = PythonOperator(
    task_id='cargar_info',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=MARVEL_dag,
)

# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32