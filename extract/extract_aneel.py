import os
import io
import requests
import pandas as pd
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# URLs corretas — dataset reorganizado por ano em 2025
ANEEL_SAMP_URLS = {
    "2024": "https://dadosabertos.aneel.gov.br/dataset/3e153db4-a503-4093-88be-75d31b002dcf/resource/ff80dd21-eade-4eb5-9ca8-d802c883940e/download/samp-2024.csv",
    "2025": "https://dadosabertos.aneel.gov.br/dataset/3e153db4-a503-4093-88be-75d31b002dcf/resource/6fac5605-5df0-469d-be08-04ee22934f60/download/samp-2025.csv",
}

def extract_aneel_samp(ano: str, url: str) -> pd.DataFrame:
    print(f"[{datetime.now().isoformat()}] Baixando SAMP {ano}...")

    response = requests.get(url, timeout=120)
    response.raise_for_status()

    df = pd.read_csv(
        io.StringIO(response.content.decode("latin-1")),
        sep=";",
        low_memory=False
    )

    df["ano_referencia"] = ano
    print(f"[{datetime.now().isoformat()}] SAMP {ano}: {len(df):,} linhas, {len(df.columns)} colunas")
    return df


def upload_to_s3(df: pd.DataFrame, ano: str, bucket: str) -> str:
    today = datetime.now().strftime("%Y/%m/%d")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"aneel/samp/ano={ano}/extracted_at={today}/samp_{ano}_{timestamp}.csv"

    print(f"[{datetime.now().isoformat()}] Enviando para s3://{bucket}/{s3_key} ...")

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8")

    s3_client = boto3.client("s3", region_name=AWS_REGION)
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
        Metadata={
            "source": "aneel-dadosabertos",
            "dataset": "samp",
            "ano": ano,
            "extracted_at": datetime.now().isoformat(),
            "row_count": str(len(df))
        }
    )

    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"[{datetime.now().isoformat()}] Upload concluído: {s3_uri}")
    return s3_uri


def run():
    print("=" * 60)
    print("EXTRAÇÃO ANEEL SAMP — INÍCIO")
    print("=" * 60)

    resultados = []

    for ano, url in ANEEL_SAMP_URLS.items():
        try:
            df = extract_aneel_samp(ano, url)

            assert len(df) > 0, f"Dataset {ano} vazio"
            assert len(df.columns) > 5, f"Estrutura inesperada no dataset {ano}"

            if ano == list(ANEEL_SAMP_URLS.keys())[0]:
                print(f"\nColunas disponíveis: {list(df.columns)}\n")
                print(f"Amostra ({ano}):")
                print(df.head(3).to_string())
                print()

            s3_uri = upload_to_s3(df, ano, S3_BUCKET)
            resultados.append({"ano": ano, "linhas": len(df), "s3_uri": s3_uri})

        except requests.exceptions.HTTPError as e:
            print(f"[ERRO] Falha no download de {ano}: {e}")
        except Exception as e:
            print(f"[ERRO] Falha inesperada em {ano}: {e}")

    print("\n" + "=" * 60)
    print("RESUMO DA EXTRAÇÃO")
    print("=" * 60)
    for r in resultados:
        print(f"  {r['ano']}: {r['linhas']:,} linhas → {r['s3_uri']}")
    print("=" * 60)


if __name__ == "__main__":
    run()
