variable "file_name" {
    type = string
    default = "training.txt"
}

variable "num" {
    type = number
    default = 1
}

variable "lst" {
    type = list(string)
    default = ["red","blue","pink"]
}

variable "tup" {
    type = tuple([ string,bool,number ])
    default = ["red",false,1]
}

variable "dict" {
    type = map
    default = {name = "Lee", age = 40}
}

locals {
  full_content = "${var.num} / ${random_pet.r_pet.id} / ${var.lst[random_integer.r_int.result]} / ${var.dict["name"]} / ${var.dict["age"]} "
  full_file_name = "files/${var.file_name}"

}

resource "local_file" "local_file_training" {
    filename = local.full_file_name
    content = local.full_content

    depends_on = [ random_pet.r_pet ]
}

output "full_content" { value = local.full_content }

# Parameters

# terraform plan -var="file_name=var_test.csv"
# terraform apply -var="file_name=var_test.csv"

# terraform plan -var-file="vars.tfvars"
# terraform apply -var-file="vars.tfvars"