# output "table_id" { value = google_bigquery_table.lhannah_table_test.*.table_id }

# output "table_csv" { value = "${join(", ", google_bigquery_table.lhannah_table_test.*.table_id)}" }