# locals {
#   source_bucket_name = module.lhannah_sink.b_n[0]
#   source_path = "adtech/"
#   sink_bucket_name = element([ for n in google_storage_bucket.bucket: n.name ], index(google_storage_bucket.bucket, "lhannah-sink-state-backup")) #"lhannah-sink-dev"
#   sink_path = google_storage_bucket_object.content_folders_backup.name #"state_backup/"
#   current_time = timestamp()
#   current_time_local = local.current_time
#   pub_sub_table_name = "${var.project}:dev_lhannah_dataset_test.${one(module.lhannah_table_dev_StorageTransferRunResults.table_name)}"

#   source_path_list = ["communications/","identities/"]
#   source_path_map = { for v in local.source_path_list : index(local.source_path_list, v) => v }

#   sink_path_list = formatlist("${local.sink_path}%s", local.source_path_list)

# }

##### perms #####

data "google_storage_transfer_project_service_account" "default" {
  project = var.project
}

data "google_storage_project_service_account" "default" {
  project = var.project
}

# storage_project_service_account

resource "google_storage_bucket_iam_member" "storage_admin_state" {
    bucket     = "bck-mstr-us-east1-tfadmin-sbx-state"
    role       = "roles/storage.admin"
    member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

    depends_on = [ module.lhannah_sink ]

}

resource "google_storage_bucket_iam_member" "storage_admin_backup" {
    bucket     = "lhannah-sink-state-backup-dev"
    role       = "roles/storage.admin"
    member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

     depends_on = [ google_storage_bucket_iam_member.storage_admin_state ]

}

# PubSub

resource "google_pubsub_topic_iam_member" "pubsub_publisher_st" {
  topic = "lhannah-ps-df-test-topic-state"
  role = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

  depends_on = [ google_pubsub_topic.topic ]

}

resource "google_pubsub_subscription_iam_member" "pubsub_subscriber_st" {
    subscription = "lhannah-ps-df-test-sub-state"
    role = "roles/pubsub.subscriber"
    member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

    depends_on = [ google_pubsub_topic_iam_member.pubsub_publisher_st ]

}

# storage_project_service_account

resource "google_pubsub_topic_iam_member" "pubsub_publisher_gs" {
    topic = "lhannah-ps-df-test-topic-state"
    role = "roles/pubsub.publisher"
    member = "serviceAccount:${data.google_storage_project_service_account.default.email_address}" #service-659189813226@gs-project-accounts.iam.gserviceaccount.com
    depends_on = [ google_pubsub_subscription_iam_member.pubsub_subscriber_st ]

}

# storage_project_service_account 2241

resource "google_pubsub_topic_iam_member" "pubsub_publisher_2241" {
    topic = "lhannah-ps-df-test-topic-state"
    role = "roles/pubsub.publisher"
    member = "serviceAccount:service-224186624166@gs-project-accounts.iam.gserviceaccount.com" 
    depends_on = [ google_pubsub_topic_iam_member.pubsub_publisher_gs ]

}

# ##### perms #####

resource "google_storage_notification" "notification" {
  bucket         = "bck-mstr-us-east1-tfadmin-sbx-state"
  payload_format = "JSON_API_V1"
  topic          = "lhannah-ps-df-test-topic-state"
  event_types    = ["OBJECT_FINALIZE", "OBJECT_METADATA_UPDATE"]
  custom_attributes = {
    new-attribute = "lhanna12-test"
  }

  depends_on = [ google_storage_bucket_iam_member.storage_admin_backup,google_pubsub_topic_iam_member.pubsub_publisher_2241 ]

}

resource "google_storage_transfer_job" "storage_transfer_event" {
  project = var.project
  description = "Copy from tfadmin"
  transfer_spec {
    gcs_data_source {
      bucket_name = "bck-mstr-us-east1-tfadmin-sbx-state"
      #path = "data-engineering/"
    }
    gcs_data_sink { 
      bucket_name = "lhannah-sink-state-backup-dev"
      #path = "state/"
    }
    transfer_options {
      overwrite_objects_already_existing_in_sink = true
      overwrite_when = "DIFFERENT"
      delete_objects_from_source_after_transfer = false
    }
  }

  event_stream {
    name = "projects/${var.project}/subscriptions/${local.lst_dict_topic_subs.state.sub}"
  }

  depends_on = [ google_storage_notification.notification ]

}

output "email" { value = data.google_storage_project_service_account.default.email_address }

# output "sink_path_list" { value = local.sink_path_list }
# output "current_time_local" { value = timestamp() }
# output "pub_sub_table_name" { value = local.pub_sub_table_name }