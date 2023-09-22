import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json
from os import path

class JsonlToBigQuery:
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def run(self, jsonl_file_path, bucket_id):
        pipeline_options = PipelineOptions(temp_location = bucket_id)
        #pipeline_options = PipelineOptions(project = 'mstr-globalbi-sbx-c730',region = 'us-east1',runner = 'DataflowRunner', staging_location = bucket_id, temp_location = bucket_id)
        
        pipeline = beam.Pipeline(options=pipeline_options)
        

        (
            pipeline
            | "Read JSONL" >> beam.io.ReadFromText(jsonl_file_path)
            | "Parse JSONL" >> beam.Map(self.parse_jsonl_row)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                self.table_id,
                dataset=self.dataset_id,
                project=self.project_id,
                # schema=schema_file_path,
                # create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

        pipeline.run().wait_until_finish()
        
        print("\n")
        print(f"{self.table_id} load complete")
        print("\n")

    def parse_jsonl_row(self, row):
        # Parse JSONL row and return as a dictionary
        json_data = json.loads(row)
        
        #print(json_data)
        
        return json_data

    # def get_table_schema(self):
    #     # Load the JSON schema from file
    #     with open(self.schema_file_path) as schema_file:
    #         schema_json = json.load(schema_file)

    #     # Define the BigQuery table schema
    #     schema = bigquery.TableSchema()
    #     for field in schema_json:
    #         schema_field = bigquery.TableFieldSchema()
    #         schema_field.name = field['name']
    #         schema_field.type = field['type']
    #         schema_field.mode = field['mode']
    #         schema.fields.append(schema_field)

    #     return schema

def dataflow_jsonl_bigquery(file_format, file_name, bucket, table_id): #schema_file_path, table_id, jsonl_file_path

    jsonl_file_path_prefix = 'C:/Users/lehannah/OneDrive - Monster_AD/Monster Files/Learning/Terraform/Udemy/terraform/files/'

    #'C:/Users/lehannah/OneDrive - Monster_AD/Monster Files/Learning/Terraform/Udemy/terraform/files/json/dev_ds_test.json'

    # Usage example
    #schema_file_path = 'bqschemas/data_control.json'
    jsonl_file_path = f'{jsonl_file_path_prefix}{file_format}/{file_name}.{file_format}'

    print("\n")
    print(jsonl_file_path)
    print("\n")

    project_id = "mstr-globalbi-sbx-c730"
    dataset_id = "dev_lhannah_dataset_test"
    bucket_id = f'gs://{bucket}/df'

    jsonl_to_bigquery = JsonlToBigQuery(project_id, dataset_id, table_id)
    jsonl_to_bigquery.run(jsonl_file_path, bucket_id)
    
if __name__ == '__main__':
    dataflow_jsonl_bigquery()