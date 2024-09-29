## Desafio Técnico

Este repositório contém a solução para o desafio técnico, com foco na construção de um pipeline de dados para coletar, transformar e analisar dados de vendas.

## Estrutura do Projeto

- **parte_um**: Documento em PDF com o design do pipeline.
- **parte_dois**: 
  - `gcs_to_bigquery.py`: Script Python para processamento de dados do Cloud Storage para o BigQuery.
  - `generate_report.sql`: Consulta SQL para gerar uma tabela de assinaturas ativas e receita total.
  - `xlsx_to_bucket_gcs.py`: Script Python para carregar dados de um arquivo Excel no GCS.
- **parte_tres**: `dag_airflow_report.py`: Arquivo de configuração do DAG do Apache Airflow para orquestração do pipeline.

## Tecnologias Utilizadas

- Google Cloud Platform (GCS, BigQuery)
- Apache Airflow
- Python