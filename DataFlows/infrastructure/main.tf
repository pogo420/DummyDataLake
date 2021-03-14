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

}