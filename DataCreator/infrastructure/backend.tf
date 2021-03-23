terraform {
  backend "gcs" {
    bucket = "dd-infra-tf-state"
    prefix = "data-creator/state/"
  }
}
