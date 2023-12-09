import json
from subprocess import run
from argparse import ArgumentParser

PROJECT_ID = 'mstr-globalbi-sbx-c730'
DATASET_ID = 'dev_lhannah_dataset_test'
TABLE_ID = 'dev_LhannahPsDfTest'
OUTPUT_SCHEMA_NAME = 'test.json'

def create_table_schema(project_id = PROJECT_ID, dataset_id = DATASET_ID, table_id = TABLE_ID, output_schema_name = OUTPUT_SCHEMA_NAME):

    bq_cmd = f'bq.cmd show --schema --format=prettyjson {project_id}:{dataset_id}.{table_id} > bqschemas/{output_schema_name}'
    bq_cmd = f'bq.cmd show --schema --format=prettyjson {project_id}:{dataset_id}.{table_id} > bqschemas/sub_to_bq_schema/{output_schema_name}'

    run(bq_cmd, shell=True, capture_output=True, text=True)
    
    print(f"Schema {output_schema_name} created from {project_id}:{dataset_id}.{table_id}")
    
    final_schema = f"bqschemas/sub_to_bq_schema/{output_schema_name}"
    
    add_fields_to_json(final_schema)
    print(f"Add {table_id} / {output_schema_name} to bigquery.tf")
    
def add_fields_to_json(output_schema_name):

    with open(output_schema_name, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write('{\n"fields": '.rstrip('\r\n') + '\n' + content)
            
    with open(output_schema_name, "a") as f:
        f.write("}")

    print(f"{output_schema_name} Updated")

if __name__ == "__main__":
    
    # Parsing arguments
    parser = ArgumentParser()
    parser.add_argument(
        '-p', '--project_id', help='Project ID of table used to create schema.', default=PROJECT_ID,
    )
    parser.add_argument(
        '-d', '--dataset_id', help='Project ID of table used to create schema.', default=DATASET_ID
    )
    parser.add_argument(
        '-t', '--table_id', help='Project ID of table used to create schema.', default=TABLE_ID
    )
    parser.add_argument(
        '-s', '--output_schema_name', help='Output BigQuery Schema in text format', default=OUTPUT_SCHEMA_NAME,
    )
    
    known_args, pipeline_args = parser.parse_known_args() 
    
    create_table_schema(vars(known_args)['project_id'],vars(known_args)['dataset_id'],vars(known_args)['table_id'],vars(known_args)['output_schema_name'])