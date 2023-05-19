module "lhannah_table_test" {
  source = "./modules/tables"
  project = var.project
  dataset_name = "lhannah_dataset_test"
  table_names = ["lhannah_table_test","lhannah_test_table"]
  global_env = "prd"
}