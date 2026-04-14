provider "google" {
  project = var.project_id
  region  = var.region
}

# Data Lake (GCS)
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

# BigQuery Datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw"
  location   = var.region
  default_table_expiration_ms = 3600000000
  delete_contents_on_destroy  = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  location   = var.region
  default_table_expiration_ms = 3600000000
  delete_contents_on_destroy  = true
}

resource "google_bigquery_dataset" "marts" {
  dataset_id = "marts"
  location   = var.region
  default_table_expiration_ms = 3600000000
  delete_contents_on_destroy  = true
}