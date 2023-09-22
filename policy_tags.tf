# the below policy tags can be used in Avro schemas, e.g. "doc": "User's personal email address #pii_email_address"
# the tag must appear at the end of the doc value
locals {
  taxonomies = {
    privacy = {
      pii = {
        policy_tags = {
          pii_email_address = { mask = "SHA256" }
          pii_fingerprint = { desc = "Of a client application, device, etc.", mask = "SHA256" }
          pii_ip_address = {}
          pii_location_address = { mask = "SHA256" }
          pii_location_geo = { desc = "Latitude, longitude, and similar fields" }
          pii_location_postalcode = {}
          pii_phone_number = {}
          pii_profile = { desc = "Profile fields that are not specifically PII, but may contain PII", mask = "SHA256" }
        }
        masked_readers = ["group:gcp-pii-maskedreader@monster.com"]
      }
    }
  }
  fine_grained_readers = [
    "group:gcp-mstr-sevenseas-devs@randstadservices.com",
    "group:gcp-mstr-monster-devs@randstadservices.com"
  ]
}

# resource "google_data_catalog_taxonomy" "taxonomies" {
#   for_each = local.taxonomies
#   project = var.project
#   display_name = "${each.key}_${var.env}"
#   activated_policy_types = ["FINE_GRAINED_ACCESS_CONTROL"]
#   region = "us"
# }

# resource "google_data_catalog_taxonomy_iam_member" "fine_grained_readers" {
#   for_each = {
#     for pair in setproduct(keys(local.taxonomies), local.fine_grained_readers)
#     : "${pair[0]}_${pair[1]}" => { taxonomy = pair[0], reader = pair[1] }
#   }
#   taxonomy = google_data_catalog_taxonomy.taxonomies[each.value.taxonomy].name
#   role = "roles/datacatalog.categoryFineGrainedReader"
#   member = each.value.reader
# }

# output "pii" {
#     value = values(google_data_catalog_taxonomy_iam_member.fine_grained_readers)[*].member
# }

# resource "google_data_catalog_policy_tag" "parents" {
#   for_each = merge([
#     for tax, tags in local.taxonomies : {for tag in keys(tags) : "${tax}_${tag}" => { taxonomy = tax, tag = tag }}
#   ]...)
#   taxonomy = google_data_catalog_taxonomy.taxonomies[each.value.taxonomy].id
#   display_name = each.value.tag
# }

# resource "google_bigquery_datapolicy_data_policy" "parents" {
#   for_each = google_data_catalog_policy_tag.parents
#   project = var.projects.sevenseas
#   location = "us"
#   data_policy_id = "${each.value.display_name}_default_mask"
#   policy_tag = each.value.name
#   data_policy_type = "DATA_MASKING_POLICY"
#   data_masking_policy { predefined_expression = "DEFAULT_MASKING_VALUE" }
# }

# resource "google_bigquery_datapolicy_data_policy_iam_member" "parents_masked_readers" {
#   for_each = merge(flatten([
#     for tax, tags in local.taxonomies : [
#       for tag, config in tags : {
#         for read in config.masked_readers : "${tax}_${tag}_${read}" => { taxonomy = tax, tag = tag, reader = read }
#       }
#     ]
#   ])...)
#   project = var.projects.sevenseas
#   location = "us"
#   data_policy_id = google_bigquery_datapolicy_data_policy.parents["${each.value.taxonomy}_${each.value.tag}"].data_policy_id
#   role = "roles/bigquerydatapolicy.maskedReader"
#   member = each.value.reader
# }


# resource "google_data_catalog_policy_tag" "children" {
#   for_each = merge(flatten([
#     for tax, parents in local.taxonomies : [
#       for parent, children in parents : {
#         for tag, config in children.policy_tags
#         : "${tax}_${tag}" => { taxonomy = tax, parent = parent, tag = tag, config = config }
#       }
#     ]
#   ])...)
#   taxonomy = google_data_catalog_taxonomy.taxonomies[each.value.taxonomy].id
#   display_name = each.value.tag
#   description = lookup(each.value.config, "desc", null)
#   parent_policy_tag = google_data_catalog_policy_tag.parents["${each.value.taxonomy}_${each.value.parent}"].id
# }

# resource "google_data_catalog_policy_tag_iam_member" "children" {
#   for_each = google_data_catalog_policy_tag.children
#   policy_tag = each.value.name
#   role = "roles/datacatalog.categoryFineGrainedReader"
#   member = "group:gcp-${each.value.display_name}@monster.com"
# }

# resource "google_bigquery_datapolicy_data_policy" "children_column_level_security" {
#   for_each = google_data_catalog_policy_tag.children
#   project = var.projects.sevenseas
#   location = "us"
#   data_policy_id = each.value.display_name
#   policy_tag = each.value.name
#   data_policy_type = "COLUMN_LEVEL_SECURITY_POLICY"
# }

# # TODO delete after applied to prod
# moved {
#   from = google_bigquery_datapolicy_data_policy.children
#   to = google_bigquery_datapolicy_data_policy.children_data_masking
# }

# resource "google_bigquery_datapolicy_data_policy" "children_data_masking" {
#   for_each = merge(flatten([
#     for tax, parents in local.taxonomies : [
#       for parent, children in parents : {
#         for tag, config in children.policy_tags : "${tax}_${tag}" => { tag = tag, config = config }
#         if lookup(config, "mask", null) != null
#       }
#     ]
#   ])...)
#   project = var.projects.sevenseas
#   location = "us"
#   data_policy_id = "${each.value.tag}_${each.value.config.mask}"
#   policy_tag = google_data_catalog_policy_tag.children[each.key].name
#   data_policy_type = "DATA_MASKING_POLICY"
#   data_masking_policy { predefined_expression = each.value.config.mask }
# }

# resource "google_bigquery_datapolicy_data_policy_iam_member" "children_masked_readers" {
#   for_each = merge(flatten([
#     for tax, parents in local.taxonomies : [
#       for parent, children in parents : [
#         for reader in children.masked_readers : {
#           for tag, config in children.policy_tags
#           : "${tax}_${tag}_${reader}" => { taxonomy = tax, tag = tag, reader = reader }
#           if lookup(config, "mask", null) != null
#         }
#       ]
#     ]
#   ])...)
#   project = var.projects.sevenseas
#   location = "us"
#   data_policy_id = google_bigquery_datapolicy_data_policy.children_data_masking["${each.value.taxonomy}_${each.value.tag}"].data_policy_id
#   role = "roles/bigquerydatapolicy.maskedReader"
#   member = each.value.reader
# }

# output "policy_tags" {
#   value = {for tag in google_data_catalog_policy_tag.children : tag.display_name => { name = tag.name }}
# }


# # temporary workaround to be removed when BigQuery allows masked policy tags on repeated records

# resource "google_data_catalog_policy_tag" "pii_repeated" {
#   taxonomy = google_data_catalog_taxonomy.taxonomies["privacy"].id
#   display_name = "pii_repeated"
# }

# resource "google_data_catalog_policy_tag" "pii_repeated_children" {
#   for_each = google_data_catalog_policy_tag.children
#   taxonomy = each.value.taxonomy
#   display_name = "${each.value.display_name}_repeated"
#   parent_policy_tag = google_data_catalog_policy_tag.pii_repeated.id
# }

# output "policy_tags_repeated" {
#   value = {
#     for tag in google_data_catalog_policy_tag.pii_repeated_children
#     : trimsuffix(tag.display_name, "_repeated") => { name = tag.name }
#   }
# }
