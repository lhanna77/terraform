module "lhannah_dataset_test" {
  source = "terraform-google-modules/bigquery/google"
  version = "~> 5.0"
  dataset_id = local.dataset_id_pick
  dataset_name = local.dataset_id_pick
  project_id = var.project
  description = local.desc_pick
  location = var.location
}

resource "google_bigquery_table" "lhannah_table_test" {
  for_each = toset(local.table_id_pick)
  deletion_protection=false
  project = var.project
  dataset_id = module.lhannah_dataset_test.bigquery_dataset.dataset_id
  table_id = each.value
  description = local.desc_pick
   time_partitioning {
      type = "DAY"
      field =  "RunDate"
    }
  schema = file("./bqschemas/LHannahTable.json")

  depends_on = [ google_bigquery_table.lhannah_table_testDepends ]

} 

resource "google_bigquery_table" "lhannah_table_testDepends" {
  deletion_protection=false
  project = var.project
  dataset_id = module.lhannah_dataset_test.bigquery_dataset.dataset_id
  table_id = "${var.table_names[0]}-depends"
  description = local.desc_pick
   time_partitioning {
      type = "DAY"
      field =  "RunDate"
    }
  schema = file("./bqschemas/LHannahTable.json")

} 

