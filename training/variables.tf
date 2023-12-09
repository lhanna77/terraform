# general

variable "location" { default = "US" }
variable "project" { default = "mstr-globalbi-sbx-c730" }
variable "region" { default = "us-east1" }
variable "env" { default = "sbx" }

locals {
  local_zone = "${var.region}-b"
}