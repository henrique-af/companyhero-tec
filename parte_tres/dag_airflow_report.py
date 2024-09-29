from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
from parte_dois.gcs_to_bigquery import (
    download_xlsx_from_gcs,
    convert_xlsx_to_parquet,
    upload_parquet_to_gcs,
    load_parquet_to_bigquery,
    delete_xlsx_from_gcs
)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 29)
}

@dag(default_args=default_args, schedule_interval='@hourly', catchup=False)
def gcs_to_bigquery_dag():

    @task
    def process_xlsx(bucket_name, source_blob_name, local_file_name, output_dir):
        download_xlsx_from_gcs(bucket_name, source_blob_name, local_file_name)

        parquet_files = convert_xlsx_to_parquet(local_file_name, output_dir)
        
        upload_parquet_to_gcs(bucket_name, parquet_files, output_dir)

        for parquet_file in parquet_files:
            table_name = os.path.splitext(os.path.basename(parquet_file))[0]
            gcs_uri = f"gs://{bucket_name}/{os.path.basename(parquet_file)}"
            load_parquet_to_bigquery(project_id, table_name, gcs_uri)

        delete_xlsx_from_gcs(bucket_name, source_blob_name)

    @task
    def run_sql_query(sql_file_path):
        try:
            with open(sql_file_path, 'r') as sql_file:
                sql_query = sql_file.read()
                sql_query = sql_query.replace(f'{project_id}', project_id)
            return sql_query
        except Exception as e:
            raise ValueError(f"Error reading SQL file: {e}")

    @task
    def execute_sql_query(sql_query):
        bq_operator = BigQueryInsertJobOperator(
            task_id='execute_sql_query',
            configuration={
                "query": {
                    "query": sql_query,
                    "useLegacySql": False,
                }
            }
        )
        bq_operator.execute(context={})

    # Variáveis do DAG
    bucket_name = Variable.get('BUCKET_NAME_VENDAS')
    source_blob_name = Variable.get('INPUT_XLSX_VENDAS')
    local_file_name = f'/tmp/{source_blob_name}'
    output_dir = Variable.get('CAMADA_BRONZE') 
    project_id = Variable.get('GCP_PROJECT_ID') 
    generate_report_sql = Variable.get('REPORT_VENDAS_SQL') 

    #Processamento do arquivo xlsx para input no BigQuery
    process_xlsx_task = process_xlsx(bucket_name, source_blob_name, local_file_name, output_dir)

    #Captura a query para gerar report
    sql_query = run_sql_query(generate_report_sql)
    
    #Roda a query criando uma nova tabela
    execute_sql_query_task = execute_sql_query(sql_query)
    
    process_xlsx_task >> execute_sql_query_task

#Criação da dag
dag_instance = gcs_to_bigquery_dag()