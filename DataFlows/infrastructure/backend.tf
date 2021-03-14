terraform {
  backend "gcs" {
    bucket = "dd-infra-tf-state"
    prefix = "dataflow/state/"
  }
}
