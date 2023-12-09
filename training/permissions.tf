# resource "google_service_account" "sa" {
#   provider = google.impersonated
#   project = var.project
#   account_id = "svc-659189813226-leehan"
#   display_name = "terraform-gcp"
#   description = "Test SA account for terraform"
# }

# resource "google_service_account_iam_binding" "service_account_iam_binding" {
#   service_account_id = google_service_account.sa.name
#   role               = "roles/editor"

#   members = [ "user:lee.hannah@Monster.co.uk" ]

#   depends_on = [ google_service_account.sa ]

# }