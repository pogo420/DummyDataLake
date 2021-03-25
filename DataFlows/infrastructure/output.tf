output "dataflow_bucket" {

value = google_storage_bucket.dataflow_bucket.id
}

output "dataflow_dataset" {

value = google_bigquery_dataset.dataflow_dataset.id
}

output "sensor_table" {

value = google_bigquery_table.dataflow_table_1.id
}