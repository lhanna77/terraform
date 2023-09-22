# general

variable "location" { default = "US" }
variable "region" { default = "us-east1" }
variable "project" { default = "mstr-globalbi-sbx-c730" }
variable "global_env" { default = "dev" }

variable "env" { 
    type = map
    default = {
        "dev" = "dev"
        "qa" = "qa"
        "prd" = "prd"
    } 
}

variable "desc" { default = "lhannah_test" }

# dataset

variable "dataset_id" { default = "lhannah_dataset_test" }

# tables

variable "table_names" { default = ["lhannah_table_test", "lhannah_table_test2", "lhannah_table_test3"] }

variable "schema_name" { default = "data_control.json" }

variable "partitioning_field" { default = "RunDate" }