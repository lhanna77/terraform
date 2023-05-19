locals {
  dataset_id_pick = var.global_env == "prd" ? "${var.env["prd"]}_${var.dataset_name}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.dataset_name}" : "${var.env["dev"]}_${var.dataset_name}"

  table_id_pick = [
    for i, name in var.table_names : var.global_env == "prd" ? "${var.env["prd"]}-${var.table_names[i]}" : var.global_env == "qa" ? "${var.env["qa"]}-${var.table_names[i]}" : "${var.env["dev"]}-${var.table_names[i]}"
  ]

  desc_pick = var.global_env == "prd" ? "${var.env["prd"]}-${var.desc}" : var.global_env == "qa" ? "${var.env["qa"]}-${var.desc}" : "${var.env["dev"]}-${var.desc}"

}
