resource "google_storage_bucket" "bucket" {
  for_each = toset(var.bucket_names)
  name     = "${each.value}-${var.global_env}"
  location = var.location
  project  = var.project


  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = local.storage_class_pick
    }
    condition {
      age = local.delete_age_pick
     }
    }

  force_destroy = var.force_destroy
  
}