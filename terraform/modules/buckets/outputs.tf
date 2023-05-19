output "bucket_name" { value = values(google_storage_bucket.bucket)[*].name }
