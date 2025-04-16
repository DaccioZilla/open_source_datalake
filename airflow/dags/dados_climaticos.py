from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pendulum
from os.path import join
import pandas as pd
from minio import Minio
import io
import os

def extrai_dados(data_interval_end):
    # intervalo de datas
    city = 'Boston'
    key = 'DZPQZ6LW9F7655AMW56DNHSMK'

    url = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
        ,f'{city}/{data_interval_end}/{ds_add(data_interval_end,7)}?unitGroup=metric&include=days&key={key}&contentType=csv'
    )

    dados = pd.read_csv(url)

    minio_config = {
        "dest_bucket":    "datalake", # This will be auto created
        "minio_endpoint": os.environ.get('MINIO_API_HOST'),
        "minio_username": os.environ.get('MINIO_ACCESS_KEY'),
        "minio_password": os.environ.get('MINIO_SECRET_KEY'),
    }

    minio_client = Minio(minio_config["minio_endpoint"],
                secure = False,
                access_key=minio_config["minio_username"],
                secret_key=minio_config["minio_password"],
                )
    def save_data_minio(df, name):    
        with io.StringIO() as output_file:
            df.to_csv(output_file)
            texto = output_file.read()
            data_bytes = texto.encode('utf-8')

        data_stream = io.BytesIO(data_bytes)
        minio_client.put_object(minio_config["dest_bucket"], f'bronze/clima/{data_interval_end}/{name}.csv', data=data_stream, length=len(data_bytes), content_type='application/csv')

    save_data_minio(dados, 'dados_brutos')
    save_data_minio(dados[['datetime','tempmin', 'temp', 'tempmax']], 'temperaturas')
    save_data_minio(dados[['datetime', 'description', 'icon']], 'condicoes')


with DAG(
    'dados_climativos',
    start_date= pendulum.datetime(2023, 6, 26, tz = 'UTC'),
    schedule_interval= '0 0 * * 1',
) as dag:
    tarefa = PythonOperator(
        task_id = 'extrai_dados',
        python_callable= extrai_dados,
        op_kwargs= {
            'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'
        }
    )

tarefa