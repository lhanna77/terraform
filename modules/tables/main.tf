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

  #depends_on = [ google_bigquery_table.lhannah_table_testDepends ]

} 

# resource "google_bigquery_table" "lhannah_table_testDepends" {
#   deletion_protection=false
#   project = var.project
#   dataset_id = var.dataset_id
#   table_id = "${var.table_names[0]}-depends"
#   description = local.desc_pick
#    time_partitioning {
#       type = "DAY"
#       field =  "RunDate"
#     }
#   schema = file("./bqschemas/${var.schema_name}")

# } 

