from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json


def captura_conta_dados():
    url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content.decode('utf-8')))
    qtd = len(df.index)
    return qtd


def valida(ti):
    qtd = ti.xcom_pull(task_ids = 'captura_conta_dados')
    if(qtd > 1000):
        return 'valido'
    return 'invalido'

with DAG('dag_gi_day_lu', start_date = datetime(2023, 11, 30),
         schedule_interval='30 * * * *', catchup=False) as dag:
    
    captura_conta_dados = PythonOperator(
        task_id = 'captura_conta_dados',
        python_callable = captura_conta_dados
    )
    
    validada = BranchPythonOperator(
        task_id = 'valida',
        python_callable = valida
    )
    
    valido = BashOperator(
        task_id = 'valido',
        bash_command = "echo 'Quantidade OK'"
    )
    
    invalido = BashOperator(
        task_id = 'invalido',
        bash_command = "echo 'Quantidade invalida'"
    )
    
    captura_conta_dados >> validada >> [valido, invalido]