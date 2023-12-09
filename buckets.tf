# module "lhannah_bucket_test" {
#   source = "./modules/buckets"
#   project = var.project
#   bucket_names = ["lhannah-modulebucket_test", "lhannah-modulebucket_test2", "lhannah-modulebucket_test3"]
#   delete_age = 1
# }

locals {

  lst_buckets = ["lhannah-sink","lhannah-sink-state-backup"]
  lst_enabled = [false,true]

}

module "lhannah_sink" {
  source = "./modules/buckets"
  project = var.project
  bucket_names = local.lst_buckets
  storage_class = "STANDARD"
  location = "us-east4"
  versioning_enabled = local.lst_enabled
}

# Create a text object in Cloud Storage
resource "google_storage_bucket_object" "terraform_backup" {
  name = "terraform_lhanna12_test.tfstate.backup"
  # Use `source` or `content`
  source       = "terraform.tfstate.backup"
  #content      = "Data as string to be uploaded"
  #content_type = "text/plain"
  bucket       = "bck-mstr-us-east1-tfadmin-sbx-state" #module.lhannah_sink.b_n[1]

  depends_on = [ module.lhannah_sink, google_storage_transfer_job.storage_transfer_event ]

}

# upload_to_storage_bucket('terraform_tfstate', "C:/Users/lehannah/OneDrive - Monster_AD/Monster Files/Learning/Terraform/Udemy/terraform/terraform.tfstate", "lhannah-sink-dev")

output "bucket_names" { value = module.lhannah_sink.b_n }
#output "bucket_state_backup" { value = element([ for n in module.lhannah_sink.name : n.name ], index(local.lst_buckets, "lhannah-sink-state-backup")) }

# output "terraform_backup_name" { value = google_storage_bucket_object.terraform_backup.name }
# output "content_folder_schema" { value = module.lhannah_sink_test.content_folder_schema }
# output "content_folder_jsonl" { value = module.lhannah_sink_test.content_folder_jsonl }