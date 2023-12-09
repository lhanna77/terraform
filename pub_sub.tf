locals {

  lst_dict_topic_subs = {
    code = {
      topic = "lhannah-ps-df-test-topic"
      sub = "lhannah-ps-df-test-sub"
    }
    file = {
      topic = "lhannah-ps-df-test-topic-file"
      sub = "lhannah-ps-df-test-sub-file"
    }
    state = {
      topic = "lhannah-ps-df-test-topic-state"
      sub = "lhannah-ps-df-test-sub-state"
    }
  }

}

resource "google_pubsub_topic" "topic" {
  for_each = local.lst_dict_topic_subs
  name = each.value.topic
}

resource "google_pubsub_subscription" "sub" {
  for_each = local.lst_dict_topic_subs
  name  = each.value.sub
  topic = each.value.topic

  # labels = {
  #   foo = "bar"
  # }

  # 20 minutes
  message_retention_duration = "86400s"
  retain_acked_messages = false

  ack_deadline_seconds = 60

  retry_policy {
    minimum_backoff = "10s"
  }

  enable_message_ordering = true

  depends_on = [google_pubsub_topic.topic]

}

output "pubsub_topics" { value = [ for n in google_pubsub_topic.topic: n.name ] }
output "pubsub_subscriptions" { value = [ for n in google_pubsub_subscription.sub: n.name ] }