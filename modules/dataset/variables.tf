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

variable "desc" { default = "lhannah-test" }

# dataset

variable "dataset_id" { default = "lhannah_dataset_test" }