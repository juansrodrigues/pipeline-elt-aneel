# Pipeline ELT: Snowflake + dbt + Airflow

[![CI](https://github.com/juansrodrigues/pipeline-elt-aneel/actions/workflows/ci.yml/badge.svg)](https://github.com/juansrodrigues/pipeline-elt-aneel/actions/workflows/ci.yml)

> Pipeline ELT end-to-end com dados reais da ANEEL — 2,67 milhões de linhas orquestradas pelo Airflow, armazenadas no Snowflake e transformadas com dbt.

## Arquitetura
ANEEL (dados abertos)
│
▼
Python Extract ──► AWS S3 (raw)
│
▼
Snowflake RAW
│
▼
dbt models
┌─────────────────┐
│ staging/        │ limpeza e tipagem
│ intermediate/   │ lógica de negócio
│ marts/          │ tabelas analíticas
└─────────────────┘
│
Apache Airflow DAG
(orquestra tudo acima)
## Stack

| Camada | Tecnologia |
|---|---|
| Extração | Python + requests |
| Armazenamento raw | AWS S3 |
| Data warehouse | Snowflake |
| Transformação | dbt (staging → intermediate → marts) |
| Orquestração | Apache Airflow 2.9 + CeleryExecutor |
| Infraestrutura local | Docker Compose |
| CI/CD | GitHub Actions |

## Dados

- **Fonte:** ANEEL — Sistema de Acompanhamento de Mercado (SAMP)
- **Volume:** 2.675.160 linhas (2024 + 2025)
- **Particionamento S3:** `aneel/samp/ano={ano}/extracted_at={data}/`

## dbt

- **3 modelos:** `stg_aneel__samp` → `int_distribuidora_mercado_mensal` → `fct_receita_distribuidora`
- **8 testes de qualidade:** `not_null` em colunas críticas do source, staging e mart
- **Lineage graph:** cadeia completa de source → mart documentada

## Como rodar localmente

```bash
# Clone o repositório
git clone https://github.com/juansrodrigues/pipeline-elt-aneel.git
cd pipeline-elt-aneel

# Configure as variáveis de ambiente
cp .env.example .env
# edite .env com suas credenciais

# Sobe o Airflow
cd airflow
docker compose --env-file ../.env up -d

# Roda o dbt
cd ../dbt
SNOWFLAKE_PASSWORD="..." dbt run
SNOWFLAKE_PASSWORD="..." dbt test
```

## Autor

**Juan de Sousa Rodrigues** — Data Engineer
[linkedin.com/in/juansrodrigues](https://linkedin.com/in/juansrodrigues)
