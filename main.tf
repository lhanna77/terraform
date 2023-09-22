terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "~> 4.0"
        }
        google-beta = {
            source = "hashicorp/google-beta"
            version = "~> 4.0"
        }
    }
    required_version = ">= 1.4.2"
}

locals {
    terraform_version = "1.4.2"
}

provider "google" {
  #alias = "impersonated"
  project = var.project
  region  = var.region
}