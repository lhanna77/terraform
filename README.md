# For pip_env

cd env
pipenv --python 3.9
pipenv install -r /c/Users/lehannah/requirements.txt   pipenv install apache-beam[gcp]==2.48.0
python --version
python dataflow_jsonl_bigquery.py
exit - deactivate env
or
py -3.9 -m pip install apache-beam[gcp]
cd env
py -3.9 dataflow_jsonl_bigquery.py
-----

## For local

3.9

py -3.9 -m pip install apache-beam[gcp]==2.48.0

3.10

pip install apache-beam==2.40.0
pip install google-cloud-storage==2.10.0

-----

### Terraform

data - Data sources allow Terraform to use information defined outside of Terraform, defined by another separate Terraform configuration, or modified by functions.
merge - merge takes an arbitrary number of maps or objects, and returns a single map or object that contains a merged set of elements from all arguments.
    expansion symbol (...) to transform the value into separate arguments.
    merge(local.service_accounts, local.service_accounts_config.cloud_scheduler)
flatten - takes a list and replaces any elements that are lists with a flattened sequence of the list contents.
lookup - retrieves the value of a single element from a map, given its key. If the given key does not exist, the given default value is returned instead.
    lookup(local.service_accounts,"name","???")
values - takes a map and returns a list containing the values of the elements in that map.
    value = values(google_storage_bucket.bucket)[*].name
formatlist - produces a list of strings by formatting a number of other values according to a specification string.
    formatlist("${local.sink_path}%s", local.source_path_list)

List
output "services" { value = tolist(google_project_service.ga[*].service) }



## Types

string-"cat"
number-234, 6.5
bool-true/false
list-sequence of value e.g. (string) =>["red", "green", "blue"]
Tuple-group non homogeneous data type > tuple([string, number, bool]) => ["dog", 23, true]
map-like key value: Dictionary > {name="Ankit", age = 32}
set-only unique values
object-complex data type

## Parameters

terraform plan -var="file_name=var_test.csv"
terraform apply -var="file_name=var_test.csv"

terraform plan -var-file="vars.tfvars"
terraform apply -var-file="vars.tfvars"

## Module Outputs
Variable key to using modules as they do the mapping.
# Lists

value = [ for n in google_pubsub_topic.topic: n.name ]
value = element([ for n in google_pubsub_subscription.sub: n.name ], 0)
value = element([ for n in google_pubsub_subscription.sub: n.name ], length(google_pubsub_subscription.sub) - 1)

#Dicts
value = { for n in google_pubsub_topic.topic: n.name => n.name }