data "terraform_remote_state" "infra" {
    backend = "gcs"
    config  = {
        bucket = "dd-infra-tf-state"
        prefix = "data-creator/state/"
    }
}

resource "google_pubsub_topic" "data_creator_topic" {
  name = var.data_creator_topic
  project = var.project
}

resource "google_pubsub_subscription" "data_creator_subscription" {
  name  = var.data_creator_subscription
  topic = google_pubsub_topic.data_creator_topic.name
  }