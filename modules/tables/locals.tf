locals {

  table_id_pick = [
    for i, name in var.table_names : var.global_env == "prd" ? "${var.env["prd"]}_${var.table_names[i]}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.table_names[i]}" : "${var.env["dev"]}_${var.table_names[i]}"
  ]

  desc_pick = var.global_env == "prd" ? "${var.env["prd"]}_${var.desc}" : var.global_env == "qa" ? "${var.env["qa"]}_${var.desc}" : "${var.env["dev"]}_${var.desc}"

}
