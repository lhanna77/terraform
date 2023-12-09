resource "google_storage_bucket" "storage_bucket_name" {
  location = var.location
  name = "bck-lh-training-bucket"

  force_destroy = true
  project = var.project
  storage_class = "STANDARD"

  labels = { env = var.env, creator = "leehann", owner = "leehann", use = "training" }

  autoclass {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }

}