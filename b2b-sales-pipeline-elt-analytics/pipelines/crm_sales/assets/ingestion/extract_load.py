# @bruin.config
# name: extract_load_crm
# type: python
# @bruin.config

import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import storage, bigquery
import pandas as pd

# ==========================================
# KONFIGURASI GOOGLE CLOUD
# ==========================================
PROJECT_ID = "de-zoomcamp-2026-01"  
BUCKET_NAME = f"{PROJECT_ID}-data-lake"
DATASET_ID = "raw"
KAGGLE_DATASET = "innocentmfa/crm-sales-opportunities"

def run_ingestion():
    print("🚀 Memulai proses Ingestion dari Kaggle ke GCP...")

    # 1. Autentikasi Kaggle
    api = KaggleApi()
    api.authenticate()

    # 2. Download Dataset dari Kaggle
    download_path = "./data/raw"
    os.makedirs(download_path, exist_ok=True)
    print(f"📥 Mengunduh dataset {KAGGLE_DATASET}...")
    api.dataset_download_files(KAGGLE_DATASET, path=download_path, unzip=True)
    
    # Inisialisasi Klien GCP
    storage_client = storage.Client(project=PROJECT_ID)
    bq_client = bigquery.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)

    # 3. Proses 5 File CSV
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
            print(f"⚠️ File {file_name} tidak ditemukan, lewati...")
            continue
            
        table_name = file_name.replace('.csv', '')
        
        # A. Upload ke GCS (Data Lake)
        blob = bucket.blob(f"raw_crm_data/{file_name}")
        blob.upload_from_filename(local_file_path)
        print(f"✅ Berhasil upload {file_name} ke GCS Bucket.")

        # B. Load ke BigQuery (Raw Layer)
        df = pd.read_csv(local_file_path)
        
        # Bersihkan nama kolom agar sesuai standar BigQuery (tidak boleh spasi/huruf besar)
        df.columns = df.columns.str.replace(' ', '_').str.lower()
        
        table_id = f"{PROJECT_ID}.{DATASET_ID}.raw_{table_name}"
        
        # Konfigurasi agar menimpa tabel jika sudah ada (Write_Truncate)
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
        )
        
        print(f"⏳ Memuat {file_name} ke BigQuery {table_id}...")
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result() # Tunggu sampai selesai
        
        print(f"✅ Selesai memuat {table_name} ke BigQuery. Total baris: {job.output_rows}")

if __name__ == "__main__":
    run_ingestion()