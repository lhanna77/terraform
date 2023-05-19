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

variable "desc" { default = "Test" }

# dataset

variable "dataset_name" { default = "dataset_test" }

# tables

variable "table_names" { default = ["table_test", "table_test2", "table_test3"] }