locals {
  
    delete_age_pick = var.storage_class == "COLDLINE" ? 365 : var.storage_class == "NEARLINE" ? 30 : var.delete_age
    storage_class_pick = var.storage_class == "COLDLINE" ? "COLDLINE" : var.storage_class == "NEARLINE" ? "NEARLINE" : var.storage_class

    buckets = var.bucket_names
    ens = var.versioning_enabled

    lst_dict_buckets = zipmap(local.buckets, local.ens)

}

resource "google_storage_bucket" "bucket" {
  for_each = local.lst_dict_buckets
  name     = "${each.key}-${var.global_env}"
  location = var.location
  project  = var.project

  versioning {
    enabled = each.value
  }

 lifecycle_rule {
    condition {
      num_newer_versions = 3
    }
    action {
      type = "Delete"
    }
  }

  # retention_policy {
  #   retention_period = 3600
  # }

  force_destroy = var.force_destroy
  
}

resource "google_storage_bucket_object" "content_folders" {
  for_each      = toset(var.storage_dirs)
  name          = each.value
  content       = "Not really a directory, but it's empty."
  bucket        = element([ for n in google_storage_bucket.bucket: n.name ], 0)

  depends_on = [ google_storage_bucket.bucket ]

}

# resource "google_storage_bucket_object" "content_folders_backup" {
#   name          = "state_backup/"
#   content       = "Not really a directory, but it's empty."
#   bucket        = element([ for n in google_storage_bucket.bucket: n.name ], index(var.bucket_names, "lhannah-sink-state-backup"))

#   depends_on = [ google_storage_bucket.bucket ]

# }

output "b_n" { value = [ for n in google_storage_bucket.bucket: n.name ] }
#output "content_folders" { value = values(google_storage_bucket_object.content_folders)[*].name }