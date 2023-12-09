terraform {

    required_version = ">= 1.4.2"

    required_providers {
    #     random = {
    #         source = "hashicorp/random"
    #         version = "~> 3.0"
    #     }

    #     local = {
    #         source = "hashicorp/local"
    #         version = "~> 2.0"
    #     }

          google = {
            source = "hashicorp/google"
            version = "~> 5.0"
          }
  }
}

locals {
    terraform_version = ">= 1.4.2"
}

provider "google" {
  alias = "impersonated"
  #credentials = ""
  project = var.project
  region  = var.region
  zone  = local.local_zone
}