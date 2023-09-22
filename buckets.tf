# module "lhannah_bucket_test" {
#   source = "./modules/buckets"
#   project = var.project
#   bucket_names = ["lhannah-modulebucket_test", "lhannah-modulebucket_test2", "lhannah-modulebucket_test3"]
#   delete_age = 1
# }

module "lhannah_bucky_test" {
  source = "./modules/buckets"
  project = var.project
  bucket_names = ["lhannah-bucky"]
  global_env = "qa"
  storage_class = "COLDLINE"
  location = "us-east4"

}

#output "bucket_name" { value = module.lhannah_bucky_test.bucket_name }