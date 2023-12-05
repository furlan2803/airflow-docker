from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
import boto3
import pandas as pd

# Defina suas credenciais AWS
aws_region_name = 'us-east-1'
aws_access_key_id='ASIAWC6ETZCLXUXOXHUS'
aws_secret_access_key='DstcQkvGk7eaigxTelS+3ceKPZxJGhNrT4y2IIZD'
aws_session_token='FwoGZXIvYXdzEB0aDDP0hpnaCqzpJOejUyLSAY67HryySjLoN7IRStTsyQeE2UH1bVDP+c8BTq4HflGCCbjAEmUGymGmBFe4TeEFqyr9mRjnbUsKowaW/jQAkIlkV2e2zRtCTom0g12uBsHp4Ac1jWL3hEG9FQxgG56HpPZY/T4egN4+PaAgy/Mxsd5YRf0LyJQKLK5bdsqW4emgPGeMacIVoZhHv2DhSz02y83oXBt03VaEVyVBIzNy3CEX7HCiLnPF0WGw2KIbkiSx//XzDug6/OINYcUpUhIh81RNyMNzGP1KP0srru4MWq/mdCiQiLyrBjItH1yMJmqUpOjJKi2QyCzDHtztOlrX+Kgmj/shJnTcMMPuZbyRiumM5jpZtfH0'

# Configuração do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    's3_connection_example',
    default_args=default_args,
    description='Um exemplo de DAG para conectar ao Amazon S3',
    schedule_interval=timedelta(days=1),
)
# Função para se conectar ao S3
def connect_to_s3():
    try:
        # Crie um cliente S3 usando o boto3
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
            aws_session_token=aws_session_token
        )
        # Faça algo com o cliente S3 (por exemplo, liste os buckets)
        response = s3_client.list_buckets()
        print("Buckets S3:")
        for bucket in response['Buckets']:
            print(f' - {bucket["Name"]}')
    except Exception as e:
        print(f"Erro ao conectar ao S3: {e}")
# Operador para chamar a função de conexão ao S3
s3_connection_task = PythonOperator(
    task_id='s3_connection_task',
    python_callable=connect_to_s3,
    dag=dag,
)

# Operador para listar buckets no S3
def list_buckets():
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
            aws_session_token=aws_session_token
        )
        response = s3_client.list_buckets()
        print("Buckets S3:")
        for bucket in response['Buckets']:
            print(f' - {bucket["Name"]}')
    except Exception as e:
        print(f"Erro ao listar buckets: {e}")

list_buckets_task = PythonOperator(
    task_id='list_buckets',
    python_callable=list_buckets,
    dag=dag,
)

# Operador para escolher um bucket específico
def choose_bucket():
    # Implemente a lógica para escolher um bucket específico
    # Pode ser manual ou baseado em lógica de seleção
    chosen_bucket = 'ponderada-airflow'
    print("Bucket Escolhido:", chosen_bucket)
    return chosen_bucket

choose_bucket_task = PythonOperator(
    task_id='choose_bucket',
    python_callable=choose_bucket,
    dag=dag,
)

# Operador para listar arquivos no bucket escolhido
def list_files_in_bucket(**kwargs):
    chosen_bucket = kwargs['ti'].xcom_pull(task_ids='choose_bucket')
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name,
        aws_session_token=aws_session_token
    )
    response = s3_client.list_objects(Bucket=chosen_bucket)
    print(f"Arquivos no Bucket {chosen_bucket}:")
    for file in response.get('Contents', []):
        print(f' - {file["Key"]}')

list_files_task = PythonOperator(
    task_id='list_files_in_bucket',
    python_callable=list_files_in_bucket,
    provide_context=True,
    dag=dag,
)

# Operador para pré-processamento
def preprocess_file(**kwargs):
    chosen_bucket = kwargs['ti'].xcom_pull(task_ids='choose_bucket')
    chosen_file = 'ponderada_airflow.csv'  # Substitua pelo arquivo específico que você escolheu

    s3_client = boto3.client(
        's3',
        aws_access_key_id='ASIAWC6ETZCLXUXOXHUS',
        aws_secret_access_key='DstcQkvGk7eaigxTelS+3ceKPZxJGhNrT4y2IIZD',
        aws_session_token='FwoGZXIvYXdzEB0aDDP0hpnaCqzpJOejUyLSAY67HryySjLoN7IRStTsyQeE2UH1bVDP+c8BTq4HflGCCbjAEmUGymGmBFe4TeEFqyr9mRjnbUsKowaW/jQAkIlkV2e2zRtCTom0g12uBsHp4Ac1jWL3hEG9FQxgG56HpPZY/T4egN4+PaAgy/Mxsd5YRf0LyJQKLK5bdsqW4emgPGeMacIVoZhHv2DhSz02y83oXBt03VaEVyVBIzNy3CEX7HCiLnPF0WGw2KIbkiSx//XzDug6/OINYcUpUhIh81RNyMNzGP1KP0srru4MWq/mdCiQiLyrBjItH1yMJmqUpOjJKi2QyCzDHtztOlrX+Kgmj/shJnTcMMPuZbyRiumM5jpZtfH0',
    )

    # Baixa o arquivo do S3
    with open(chosen_file, 'wb') as f:
        s3_client.download_fileobj(Bucket=chosen_bucket, Key=chosen_file, Fileobj=f)

    # Carrega o arquivo em um DataFrame pandas
    df = pd.read_csv(chosen_file, encoding='latin1')  # Ou tente encoding='utf-8'

    # Verifica se há dados nulos
    if df.isnull().values.any():
        print("Dados nulos encontrados. Preenchendo com 'N/A'...")
        df.fillna('N/A', inplace=True)

        # Salva o DataFrame de volta no S3
        df.to_csv(chosen_file, index=False)
        with open(chosen_file, 'rb') as f:
            s3_client.upload_fileobj(f, Bucket=chosen_bucket, Key=chosen_file)
        print("Arquivo pré-processado salvo no S3.")
        return 'branch_has_nulls'
    else:
        print("Não foram encontrados dados nulos. Arquivo válido.")
        return 'branch_no_nulls'

preprocess_file_task = PythonOperator(
    task_id='preprocess_file',
    python_callable=preprocess_file,
    provide_context=True,
    dag=dag,
)


# Operador para bifurcação com base na presença de dados nulos
def decide_branch(**kwargs):
    task_instance = kwargs['ti']
    # Recebe a saída da tarefa preprocess_file
    result = task_instance.xcom_pull(task_ids='preprocess_file')
    if result == 'branch_has_nulls':
        return 'handle_nulls_task'
    else:
        return 'handle_no_nulls_task'

branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=decide_branch,
    provide_context=True,
    dag=dag,
)

# Tarefas específicas para cada ramo
def handle_nulls():
    print("Lógica para lidar com dados nulos...")

handle_nulls_task = PythonOperator(
    task_id='handle_nulls_task',
    python_callable=handle_nulls,
    dag=dag,
)

def handle_no_nulls():
    print("Lógica para lidar com arquivo válido...")

handle_no_nulls_task = PythonOperator(
    task_id='handle_no_nulls_task',
    python_callable=handle_no_nulls,
    dag=dag,
)

# Defina a ordem de execução das tarefas
s3_connection_task >> list_buckets_task >> choose_bucket_task >> list_files_task >> preprocess_file_task >> branch_task
branch_task >> [handle_nulls_task, handle_no_nulls_task]

if __name__ == "__main__":
    dag.cli()