locals {
  dataset_id_pick = var.global_env == "prd" ? "${var.env["prd"]}_${var.dataset_id}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.dataset_id}" : "${var.env["dev"]}_${var.dataset_id}"

  desc_pick = var.global_env == "prd" ? "${var.env["prd"]}_${var.desc}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.desc}" : "${var.env["dev"]}_${var.desc}"

}
