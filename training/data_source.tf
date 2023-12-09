data "local_file" "source_file_name" {
  filename = local_file.local_file_training.filename
}

output "source_file_name" { value = data.local_file.source_file_name.filename }