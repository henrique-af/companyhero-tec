import os
import pandas as pd
import logging
from google.cloud import storage
from google.cloud import bigquery

def download_xlsx_from_gcs(bucket_name, source_blob_name, local_file_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(local_file_name)
    logging.info(f"Arquivo {source_blob_name} baixado para {local_file_name}.")

def convert_xlsx_to_parquet(local_file_name, output_dir):
    xlsx = pd.ExcelFile(local_file_name)
    parquet_files = []
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name)
        parquet_file = os.path.join(output_dir, f"{sheet_name}.parquet")
        df.to_parquet(parquet_file)
        parquet_files.append(parquet_file)
        logging.info(f"Aba {sheet_name} convertida para {parquet_file}.")
    return parquet_files

def upload_parquet_to_gcs(bucket_name, parquet_files, destination_dir):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    for parquet_file in parquet_files:
        destination_blob_name = os.path.join(destination_dir, os.path.basename(parquet_file))
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(parquet_file)
        logging.info(f"Arquivo {parquet_file} no bucket {bucket_name}.")

def load_parquet_to_bigquery(dataset_id, table_name, gcs_uri):
    client = bigquery.Client()
    table_id = f"{dataset_id}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET
    )
    
    load_job = client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )
    
    load_job.result()
    logging.info(f"Tabela {table_name} criada no BigQuery com dados de {gcs_uri}.")

def delete_xlsx_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.delete()
    logging.info(f"Arquivo XLSX {source_blob_name} deletado do bucket.")