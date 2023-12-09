# locals {
#   source_bucket_name = "bck-mstr-datastage-dev-avro-schemas"
#   source_path = "adtech/"
#   sink_bucket_name = one(module.lhannah_sink_test.bucket_name) #"lhannah-sink-dev"
#   sink_path = module.lhannah_sink_test.content_folder_schema #"storage_transfer_sink/"
#   current_time = timestamp()
#   current_time_local = local.current_time
#   pub_sub_table_name = "${var.project}:dev_lhannah_dataset_test.${one(module.lhannah_table_dev_StorageTransferRunResults.table_name)}"

#   source_path_list = ["communications/","identities/"]
#   source_path_map = { for v in local.source_path_list : index(local.source_path_list, v) => v }

#   sink_path_list = formatlist("${local.sink_path}%s", local.source_path_list)

# }

# data "google_storage_transfer_project_service_account" "default" {
#   project = var.project
# }

# data "google_storage_project_service_account" "default" {  
#   project = var.project
# }

# ##### Permissions #####

# resource "google_storage_bucket_iam_member" "bucket_admin" {
#   bucket     = local.sink_bucket_name
#   role       = "roles/storage.admin"
#   member     = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

#     depends_on = [module.lhannah_sink_test.bucket_name]

# }

# resource "google_pubsub_topic_iam_member" "pubsub_publisher" {
#   topic = google_pubsub_topic.topic.id
#   role = "roles/pubsub.publisher"
#   member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

#   depends_on = [google_pubsub_topic.topic]

# }

# resource "google_pubsub_subscription_iam_member" "pubsub_subscriber" {
#   subscription = google_pubsub_subscription.sub.id
#   role = "roles/pubsub.subscriber"
#   member = "serviceAccount:${data.google_storage_transfer_project_service_account.default.email}" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

#   depends_on = [google_pubsub_topic.topic]

# }

# resource "google_bigquery_table_iam_member" "bigquery_editor" {
#   project = var.project
#   dataset_id = "dev_lhannah_dataset_test"
#   table_id = "dev_StorageTransferRunResults"
#   role = "roles/bigquery.dataEditor"
#   member = "serviceAccount:service-659189813226@gcp-sa-pubsub.iam.gserviceaccount.com" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com

#   depends_on = [module.lhannah_table_dev_StorageTransferRunResults.table_name]

# }

# # resource "google_storage_bucket_iam_member" "storage_transfer_pubsub" {
# #   bucket     = local.sink_bucket_name
# #   role       = "roles/storage.admin"
# #   member = "serviceAccount:service-659189813226@gcp-sa-pubsub.iam.gserviceaccount.com" #project-659189813226@storage-transfer-service.iam.gserviceaccount.com
# # }

# ##### Permissions #####

# resource "google_pubsub_topic" "topic" {
#   name = "storage-transfer-notification-topic"
# }

# resource "google_pubsub_subscription" "sub" {
#   name  = "storage-transfer-notification-sub"
#   topic = google_pubsub_topic.topic.id

#   # labels = {
#   #   foo = "bar"
#   # }

#   # 20 minutes
#   message_retention_duration = "600s"
#   retain_acked_messages = false

#   ack_deadline_seconds = 60

#   retry_policy {
#     minimum_backoff = "10s"
#   }

#   enable_message_ordering = true
# }

# resource "google_pubsub_subscription" "sub_bq" {
#   name  = "storage-transfer-notification-sub-bq"
#   topic = google_pubsub_topic.topic.id

#   bigquery_config {
#     table = local.pub_sub_table_name #"mstr-globalbi-sbx-c730:dev_lhannah_dataset_test.dev_StorageTransferRunResults"
#     #use_topic_schema = true
#     #drop_unknown_fields = true
#   }

#   depends_on = [google_bigquery_table_iam_member.bigquery_editor]
# }

# resource "google_storage_transfer_job" "storage_transfer" {
#   for_each = local.source_path_map
#   project = var.project
#   description = "Copy of copy from AWS"
#   transfer_spec {
#     gcs_data_source {
#       bucket_name = local.source_bucket_name
#       path = each.value #local.source_path
#     }
#     gcs_data_sink { 
#       bucket_name = local.sink_bucket_name 
#       path = local.sink_path_list[each.key] #local.sink_path
#     }
#     transfer_options {
#       overwrite_objects_already_existing_in_sink = true
#       overwrite_when = "DIFFERENT"
#       delete_objects_unique_in_sink              = true
#       delete_objects_from_source_after_transfer  = false
#     }
#   }

#   # event_stream {
#   #   name = "projects/${var.project}/subscriptions/${google_pubsub_subscription.sub.id}"
#   # }

#   schedule {
#     schedule_start_date {
#       year  = formatdate("YYYY",local.current_time_local)
#       month = formatdate("M",local.current_time_local)
#       day   = formatdate("D",local.current_time_local)
#     }

#   #   start_time_of_day {
#   #     hours   = formatdate("H",local.current_time_local)+12
#   #     minutes = formatdate("m",local.current_time_local)+1
#   #     seconds = 0
#   #     nanos   = 0
#   #   }

#   #   schedule_end_date {
#   #     year  = formatdate("YYYY",local.current_time_local)
#   #     month = formatdate("M",local.current_time_local)
#   #     day   = formatdate("D",local.current_time_local)
#   #   }

#   }

#   notification_config {
#     pubsub_topic  = google_pubsub_topic.topic.id
#     event_types   = [
#       "TRANSFER_OPERATION_SUCCESS",
#       "TRANSFER_OPERATION_FAILED"
#     ]
#     payload_format = "JSON"
#   }

#   depends_on = [module.lhannah_sink_test.content_folder_schema]
# }

# output "sink_path_list" { value = local.sink_path_list }
# output "current_time_local" { value = timestamp() }
# output "pub_sub_table_name" { value = local.pub_sub_table_name }