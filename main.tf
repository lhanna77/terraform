terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "~> 5.0"
        }
        google-beta = {
            source = "hashicorp/google-beta"
            version = "~> 5.0"
        }
    }
    
    required_version = ">= 1.4.2"
}

locals {
    terraform_version = ">= 1.4.2"
}

provider "google" {
  #alias = "impersonated"
  #credentials = ""
  project = var.project
  region  = var.region
  zone  = local.local_zone
}