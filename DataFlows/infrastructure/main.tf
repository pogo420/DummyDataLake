data "terraform_remote_state" "infra" {
    backend = "gcs"
    config  = {
        bucket = "dd-infra-tf-state"
        prefix = "dataflow/state/"
    }
}

resource "google_storage_bucket" "dataflow_bucket" {
  name          = var.dataflow_bucket
  location      = var.location
    lifecycle {
      prevent_destroy =   false
    }
}

resource "google_bigquery_dataset" "dataflow_dataset" {
  dataset_id                  = var.dataflow_dataset
  friendly_name               = var.dataflow_dataset
  description                 = "This is dataset for all dataflow activities"
  location                    = var.location

  lifecycle {
    prevent_destroy =   false
  }
}

resource "google_bigquery_table" "dataflow_table_1" {
  dataset_id = google_bigquery_dataset.dataflow_dataset.dataset_id
  table_id   = var.sensor_data_table
  deletion_protection = false
  lifecycle {
      prevent_destroy =   false
    }
}