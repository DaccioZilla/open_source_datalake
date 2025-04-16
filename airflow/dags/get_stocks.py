import yfinance
from airflow.decorators import dag, task
from airflow.macros import ds_add
from minio import Minio
import os
import io
import pendulum
from time import sleep

TICKERS = [
            "AAPL",
            "MSFT",
            "GOOG",
            "TSLA"
            ]

@task()
def get_history(ticker, ds = None, ds_nodash = None):
    hist = yfinance.Ticker(ticker).history(
                period ="1d",
                interval = "1h",
                start = ds_add(ds, -1),
                end = ds,
                prepost = True
    )

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

    with io.StringIO() as output_file:
        hist.to_csv(output_file)
        texto = output_file.read()
        data_bytes = texto.encode('utf-8')

    data_stream = io.BytesIO(data_bytes)
    minio_client.put_object(minio_config["dest_bucket"], f'bronze/stocks/{ticker}/{ticker}_{ds_nodash}.csv', data=data_stream, length=len(data_bytes), content_type='application/csv')
    sleep(10)

@dag(
    schedule_interval= '0 0 * * 2-6', #Cron expression Meia noite [0 0], todos os dias do mês [* *], de terça a sábado [2-6]
    start_date= pendulum.datetime(2023, 11, 1, tz='UTC'),
    catchup= True  
    ) 
def get_stocks_dag():
    for ticker in TICKERS:
        get_history.override(task_id = ticker)(ticker)

dag = get_stocks_dag()