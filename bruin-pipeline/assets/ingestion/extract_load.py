"""@bruin
name: extract_load_crm
type: python
requirements_file: ../../requirements.txt
@bruin"""

import os
from dotenv import load_dotenv
load_dotenv()
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage, bigquery
import pandas as pd

PROJECT_ID = "de-zoomcamp-2026-01"  
BUCKET_NAME = f"{PROJECT_ID}-data-lake"
DATASET_ID = "raw"
KAGGLE_DATASET = "innocentmfa/crm-sales-opportunities"

def run_ingestion():
    api = KaggleApi()
    api.authenticate()

    download_path = "./data/raw"
    os.makedirs(download_path, exist_ok=True)
    api.dataset_download_files(KAGGLE_DATASET, path=download_path, unzip=True)
    
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    files_to_process = [
        "accounts.csv", 
        "products.csv", 
        "sales_pipeline.csv", 
        "sales_teams.csv",
        "dictionary.csv"
    ]

    for file_name in files_to_process:
        local_file_path = os.path.join(download_path, file_name)
        if not os.path.exists(local_file_path):
            continue
            
        table_name = file_name.replace('.csv', '')
        
        blob = bucket.blob(f"raw_crm_data/{file_name}")
        blob.upload_from_filename(local_file_path)

        df = pd.read_csv(local_file_path)
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

if __name__ == "__main__":
    run_ingestion()