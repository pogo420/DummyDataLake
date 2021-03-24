output "data_creator_topic" {

value = google_pubsub_topic.data_creator_topic.id
}

output "data_creator_subscription" {

value = google_pubsub_subscription.data_creator_subscription.id
}