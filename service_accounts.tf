locals {
  service_accounts_config = {

    cloud_scheduler = {
      account_id = "svc-mstr-datastage-sbx-clsched"
      display_name = "Cloud Scheduler service account"
      roles = {
        datastage = { bigquery = ["user"], cloudfunctions = ["invoker"], run = ["invoker"] }
        sevenseas = { bigquery = ["dataViewer"] }
      }
    }

  }

  service_accounts = {
    name = "Cloud Scheduler service account"
    sa = "lee.hannah@monster.co.uk"
  }
}

# resource "google_service_account" "service_accounts" {
#   project = var.project
#   account_id = "svc-lhannah-ps-df-test-sa"
#   display_name = "lhannah test sa"
# }

# resource "google_project_iam_member" "pubsub_subscriber" {
#   project = var.project
#   role    = "roles/pubsub.subscriber"
#   member  = "serviceAccount:${google_service_account.service_accounts.email}"
# }

# output "service_accounts" {
#   value = merge(local.service_accounts, local.service_accounts_config.cloud_scheduler)
#   #value = lookup(local.service_accounts,"name","???")
# }

# resource "google_service_account" "service_accounts" {
#   provider = google.impersonated
#   for_each = local.service_accounts_config
#   project = var.project #lookup(each.value, "project", var.projects.datastage)
#   account_id = each.value.account_id
#   display_name = each.value.display_name
# }

# resource "google_project_iam_member" "service_accounts" {
#   provider = google.impersonated
#   for_each = merge(flatten([
#     for sa, config in local.service_accounts_config : [
#       for project, products in config.roles : [
#         for product, roles in products : {
#           for role in roles : "${sa}_${project}_${product}_${role}" =>
#           { service_account = sa, project = project, product = product, role = role }
#         }
#       ]
#     ]
#   ])...)
#   project = var.projects[each.value.project]
#   role = "roles/${each.value.product}.${each.value.role}"
#   member = "serviceAccount:${google_service_account.service_accounts[each.value.service_account].email}"
# }



# data "google_storage_project_service_account" "default" {}
# data "google_storage_transfer_project_service_account" "default" {}
# data "google_client_openid_userinfo" "tfadmin" { provider = google.impersonated }