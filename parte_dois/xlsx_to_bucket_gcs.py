import os
from google.cloud import storage
import logging

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):    
    # Valida se o arquivo existe
    if not os.path.isfile(source_file_name):
        logging.error(f"Arquivo {source_file_name} não encontrado.")
        return

    try:
        # Inicializa conexão
        storage_client = storage.Client()
        
        # Acessa o bucket
        bucket = storage_client.bucket(bucket_name)
        
        # Blob
        blob = bucket.blob(destination_blob_name)
        
        # Upload
        blob.upload_from_filename(source_file_name)
        
        logging.info(f"Arquivo {source_file_name} enviado para o bucket {bucket_name} como {destination_blob_name}.")
    
    except Exception as e:
        logging.error(f"Erro ao fazer upload do arquivo: {e}")


# Upload
bucket_name = ''
source_file_name = ''
destination_blob_name = ''

upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
