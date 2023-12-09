resource "random_integer" "r_int" {
    min = 0
    max = 2

    lifecycle {
      #create_before_destroy = true
      #prevent_destroy = true
      ignore_changes = [ min, max ]
    }

}

resource "random_string" "r_str" {
    length = 100
}

resource "random_pet" "r_pet" {
    length = 2
}

output "r_int" { value = random_integer.r_int.result }
output "r_str" { value = random_string.r_str.result }
output "r_pet" { value = random_pet.r_pet.id }