locals {

  table_id_pick = [
    for i, name in var.table_names : var.global_env == "prd" ? "${var.env["prd"]}_${var.table_names[i]}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.table_names[i]}" : "${var.env["dev"]}_${var.table_names[i]}"
  ]

  desc_pick = var.global_env == "prd" ? "${var.env["prd"]}_${var.desc}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.desc}" : "${var.env["dev"]}_${var.desc}"

}

resource "google_bigquery_table" "lhannah_table_test" {
  for_each = toset(local.table_id_pick)
  deletion_protection=false
  project = var.project
  dataset_id = var.dataset_id
  table_id = each.value
  description = local.desc_pick
   time_partitioning {
      type = "DAY"
      field =  var.partitioning_field
    }
  schema = file("./bqschemas/${var.schema_name}")

} 

output "t_n" { value = [ for n in google_bigquery_table.lhannah_table_test: n.table_id ] }

# output "table_csv" { value = "${join(", ", google_bigquery_table.lhannah_table_test.*.table_id)}" }

# output "dataset_id" {
#   value = lhannah_dataset_test.dataset_id
#   }