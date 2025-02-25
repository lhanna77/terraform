#!/bin/bash

sh_dataset=LookupTables
sh_table=BudgetRates
sh_schema=budget_rates.json
sh_from_file_path="files/json/$sh_table.json"
sh_bq_table="dev_$sh_table"

 Other commands

# Delete and re-create (Update python_version = "3.9" in Pipfile)
# pipenv --rm
# Creates from pip file
# pipenv install 
# Creates - no pip file
# pipenv --python 3.12

# Lock the dependencies
# pipenv lock -r
# Python version
# python --version
# Setup new package from existing
# <copy package into new folder - e.g. c/Users/lehannah/my_project_test>
# pipenv install --ignore-pipfile #ignore the Pipfile for installation and use what’s in the Pipfile.lock

#alias python="C:/Users/lehannah/AppData/Local/Programs/Python/Python310/python.exe"

cd .

f=$1

if [[ -z $1 ]];
then 
    echo "No filename passed."
else
    echo "Filename passed = $f"
    # Create new folder for project
    new_dir="pipenv/$f"
    mkdir $new_dir
    echo "Dir created = $new_dir"

    cd $new_dir

    # Set python version
    pipenv --python 3.12

    # Create requirements (full, probably need to filter list) / install packages
    # pipenv install requests
    pip freeze > requirements.txt

    # Install packages from requirements
    # pipenv install -r requirements.txt

    # Create python file
    touch main.py
    touch __init__.py

    # Activate
    pipenv shell
    # Check security vulnerabilities
    pipenv check
    # Check dependency graph
    # pipenv graph
    # Sub-dependencies with the parent that requires it
    # pipenv graph --reverse
    # Deactivate
    # exit

    # Remove
    # pipenv --rm
    # cd ..
    # rm -r <>

fi

