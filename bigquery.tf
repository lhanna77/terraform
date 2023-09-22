module "lhannah_dataset_test" {
    source = "./modules/dataset"
    project = var.project
    dataset_id = "lhannah_dataset_test"
}

# module "lhannah_table_schema_test" {
#   source = "./modules/tables"
#   project = var.project
#   dataset_id = module.lhannah_dataset_test.dataset_id
#   table_names = ["schema_test"]
#   schema_name = "personal_details.json"
#   global_env = "dev"
# }

module "lhannah_table_dev_ds_test" {
  source = "./modules/tables"
  project = var.project
  dataset_id = module.lhannah_dataset_test.dataset_id
  table_names = ["DataControl"]
  schema_name = "data_control.json"
  global_env = "dev"
}

module "lhannah_table_dev_ds_test_backup" {
  source = "./modules/tables"
  project = var.project
  dataset_id = module.lhannah_dataset_test.dataset_id
  table_names = ["DataControl_backup"]
  schema_name = "data_control.json"
  global_env = "dev"
}

module "lhannah_table_dev_RatePlanChargeTier" {
  source = "./modules/tables"
  project = var.project
  dataset_id = module.lhannah_dataset_test.dataset_id
  table_names = ["RatePlanChargeTier"]
  schema_name = "rate_plan_charge_tier.json"
  global_env = "dev"
  partitioning_field = "CreatedDate"
}

module "lhannah_table_dev_JobAppliesCurrent" {
  source = "./modules/tables"
  project = var.project
  dataset_id = module.lhannah_dataset_test.dataset_id
  table_names = ["JobAppliesCurrent"]
  schema_name = "job_applies_current.json"
  global_env = "dev"
  partitioning_field = "ingestionTime"
}

#output "dataset_id" { value = module.lhannah_dataset_test.dataset_id }