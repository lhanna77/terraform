resource "google_bigquery_dataset" "bq_dataset" {
    project = var.project
    dataset_id = local.dataset_id_pick
    description = local.desc_pick
    location = var.location
    
}