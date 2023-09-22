from google.cloud import bigquery
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema,TableFieldSchema
import json

def write_data_bigquery(table_id, uri, source_format):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = table_id

    job_config = bigquery.LoadJobConfig(
        
        schema=get_table_schema(schema_file_path = r'C:\Users\lehannah\OneDrive - Monster_AD\Monster Files\Learning\Terraform\Udemy\terraform\bqschemas\data_control.json'),
        
        # schema=[
        #     bigquery.SchemaField("Project", "STRING"),
        #     bigquery.SchemaField("Dataset", "STRING"),
        #     bigquery.SchemaField("TableName", "STRING"),
        #     bigquery.SchemaField("RunDate", "TIMESTAMP"),
        #     bigquery.SchemaField("ETLDate", "TIMESTAMP"),
        #     bigquery.SchemaField("FullLoad", "BOOLEAN"),
        # ],
        #autodetect=True, https://cloud.google.com/bigquery/docs/nested-repeated
        source_format=source_format
    )

    uri = uri

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",  # Must match the destination dataset location.
        job_config=job_config,
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print(f"Loaded {destination_table.num_rows} rows to {destination_table.table_id}.")
    
def get_table_schema(schema_file_path):
    # Load the JSON schema from file
    with open(schema_file_path) as schema_file:
        schema_json = json.load(schema_file)

    # Define the BigQuery table schema
    schema = TableSchema()
    for field in schema_json:
        schema_field = TableFieldSchema()
        schema_field.name = field['name']
        schema_field.type = field['type']
        schema_field.mode = field['mode']
        schema.fields.append(schema_field)

    return schema