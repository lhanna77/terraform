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

variable "storage_dirs" {
  type    = list(string)
  default = ["storage_transfer_sink/schema/", "storage_transfer_sink/jsonl/", "df/", "state/"]
}

variable "bucket_names" { default = ["lhannah-bucket-test", "lhannah-bucket-test2"] }
variable "storage_class" { default = "STANDARD" }
variable "versioning_enabled" { default = false }

variable "delete_age" {
    type = string
    default="7"
    description= "Minimum age of an object in days to satisfy the condition, default is 10 years"
}

variable "force_destroy" {
    type = bool
    description = "Delete the bucket and all contained objects"
    default = true
}