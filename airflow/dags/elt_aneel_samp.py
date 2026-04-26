import os
import sys
import io
import logging
import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Logging ─────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ── Configuração via variáveis de ambiente ───────────────────────────────────
S3_BUCKET   = os.getenv("S3_BUCKET")
AWS_REGION  = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

ANEEL_URLS = {
    "2024": "https://dadosabertos.aneel.gov.br/dataset/3e153db4-a503-4093-88be-75d31b002dcf/resource/ff80dd21-eade-4eb5-9ca8-d802c883940e/download/samp-2024.csv",
    "2025": "https://dadosabertos.aneel.gov.br/dataset/3e153db4-a503-4093-88be-75d31b002dcf/resource/6fac5605-5df0-469d-be08-04ee22934f60/download/samp-2025.csv",
}

# ── Funções de cada task ─────────────────────────────────────────────────────

def extract_and_load(ano: str, **context):
    """
    Task: faz o download do SAMP de um ano específico
    e faz o upload para o S3 particionado por data de execução.
    """
    url = ANEEL_URLS[ano]
    execution_date = context["ds"]  # data de execução do DAG (YYYY-MM-DD)

    log.info(f"Iniciando extração SAMP {ano} | execution_date={execution_date}")
    log.info(f"URL: {url}")

    # 1. Download
    response = requests.get(url, timeout=180)
    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.content.decode("latin-1")),
        sep=";",
        low_memory=False
    )
    df["ano_referencia"]   = ano
    df["extracted_at"]     = execution_date

    log.info(f"SAMP {ano} baixado: {len(df):,} linhas, {len(df.columns)} colunas")

    # 2. Validação
    assert len(df) > 0,        f"Dataset {ano} vazio — abortando"
    assert len(df.columns) > 5, f"Estrutura inesperada no dataset {ano}"

    # 3. Upload S3 — particionado por ano e data de execução
    year, month, day = execution_date.split("-")
    s3_key = (
        f"aneel/samp/ano={ano}/"
        f"extracted_at={year}/{month}/{day}/"
        f"samp_{ano}_{year}{month}{day}.csv"
    )

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8")

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "source":       "aneel-dadosabertos",
            "dataset":      "samp",
            "ano":          ano,
            "extracted_at": execution_date,
            "row_count":    str(len(df)),
        }
    )

    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    log.info(f"Upload concluído: {s3_uri}")

    # Retorna o URI — fica disponível para tasks seguintes via XCom
    return {"ano": ano, "linhas": len(df), "s3_uri": s3_uri}


def log_conclusao(**context):
    """
    Task final: agrega os resultados das tasks anteriores via XCom
    e loga um resumo da execução.
    """
    ti = context["ti"]

    resultado_2024 = ti.xcom_pull(task_ids="extract_aneel_2024")
    resultado_2025 = ti.xcom_pull(task_ids="extract_aneel_2025")

    log.info("=" * 50)
    log.info("RESUMO DA EXECUÇÃO")
    log.info("=" * 50)

    total_linhas = 0
    for resultado in [resultado_2024, resultado_2025]:
        if resultado:
            log.info(f"  {resultado['ano']}: {resultado['linhas']:,} linhas → {resultado['s3_uri']}")
            total_linhas += resultado["linhas"]

    log.info(f"  Total: {total_linhas:,} linhas carregadas no S3")
    log.info("=" * 50)


# ── Definição do DAG ─────────────────────────────────────────────────────────

default_args = {
    "owner":            "juan",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="elt_aneel_samp",
    description="Extrai dados SAMP da ANEEL e carrega no S3 — pipeline ELT portfólio",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule_interval="0 6 1 * *",  # todo dia 1 de cada mês às 06h
    catchup=False,
    tags=["aneel", "s3", "elt", "portfólio"],
) as dag:

    task_extract_2024 = PythonOperator(
        task_id="extract_aneel_2024",
        python_callable=extract_and_load,
        op_kwargs={"ano": "2024"},
    )

    task_extract_2025 = PythonOperator(
        task_id="extract_aneel_2025",
        python_callable=extract_and_load,
        op_kwargs={"ano": "2025"},
    )

    task_log = PythonOperator(
        task_id="log_conclusao",
        python_callable=log_conclusao,
    )

    # Dependências: as duas extrações rodam em paralelo,
    # o log só roda quando ambas terminarem
    [task_extract_2024, task_extract_2025] >> task_log
