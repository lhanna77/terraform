#######################################################################################

#!/bin/bash

######################################################################################

# Author        : DevOps Made Easy
# Email         : devopsmadeeasy@gmail.com
# Description   : Terraform plan script
# Documentation : https://www.terraform.io/docs/commands/plan.html

######################################################################################

# Export Path Variable
# export PATH=$PATH

######################################################################################

# If statement to ensure a user has provided a Terraform folder path
# if [[ -z "$1" ]]; then
# echo ""
# echo "You have not provided a Terraform path."
# echo "SYNTAX = ./plan.sh <PATH>"
# echo "EXAMPLE = ./plan.sh terraform/instance"
# echo ""
# exit
# fi

######################################################################################

# The Init command is used to initialize a working directory containing Terraform configuration files.
# This is the first command that should be run after writing a new Terraform configuration
terraform init #-upgrade

#The Get command is used to download and update modules mentioned in the root module.
terraform get

#The Plan command is used to create an execution plan
terraform plan

terraform apply -auto-approve

######################################################################################