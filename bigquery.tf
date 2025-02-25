locals {
  lst_dict_bq_tables = {
    data_control = {
      table = "DataControl"
      schema = "data_control.json"
      partition = "RunDate"
    }
    data_control_backup = {
      table = "DataControl_backup"
      schema = "data_control.json"
      partition = "RunDate"
    }
    storage_transfer_run_results = {
      table = "StorageTransferRunResults"
      schema = "storage_transfer_run_results.json"
      partition = "RunDate"
    }
    lhannah_ps_df_test = {
      table = "LhannahPsDfTest"
      schema = "lhannah_ps_df_test.json"
      partition = "timestamp"
    }
    budget_rates = {
      table = "BudgetRates"
      schema = "budget_rates.json"
      partition = "DateAdded"
    }
    # weir_equipment = {
    #   table = "weir_equipment"
    #   schema = "weir_equipment.json"
    #   partition = "InstallationDate"
    # }
    # weir_staging_equipment = {
    #   table = "weir_staging_equipment"
    #   schema = "weir_staging_equipment.json"
    #   partition = "InstallationDate"
    # }
    # weir_staging_equipment_jan = {
    #   table = "weir_staging_equipment_jan"
    #   schema = "weir_staging_equipment.json"
    #   partition = "InstallationDate"
    # }
    # weir_staging_equipment_mar = {
    #   table = "weir_staging_equipment_mar"
    #   schema = "weir_staging_equipment.json"
    #   partition = "InstallationDate"
    # }
    # weir_staging_equipment_may = {
    #   table = "weir_staging_equipment_may"
    #   schema = "weir_staging_equipment.json"
    #   partition = "InstallationDate"
    # }
  }
}

module "lhannah_dataset_test" {
    source = "./modules/dataset"
    project = var.project
    dataset_id = "lhannah_dataset_test"
}

module "lhannah_tables_dev" {
  for_each = local.lst_dict_bq_tables
  source = "./modules/tables"
  project = var.project
  dataset_id = module.lhannah_dataset_test.dataset_id
  table_names = [each.value.table]
  schema_name = each.value.schema
  global_env = "dev"
  partitioning_field = each.value.partition
}

output "dataset_name" { value = module.lhannah_dataset_test.dataset_id }
output "table_names" { value = [ for m in module.lhannah_tables_dev : m.t_n[0] ] }



