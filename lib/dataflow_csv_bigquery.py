import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json
from pathlib import Path

class CsvToBigQuery:
    def __init__(self, project_id, dataset_id, table_id):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    def run(self, csv_file_path, schemas_file_path):
        pipeline_options = PipelineOptions()
        pipeline = beam.Pipeline(options=pipeline_options)

        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(csv_file_path)
            | "Transform to Dict" >> beam.Map(self.parse_csv_row)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=self.table_id,
                dataset=self.dataset_id,
                project=self.project_id,
                schema=schemas_file_path,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        pipeline.run().wait_until_finish()

    def parse_csv_row(self, row):
        # Split the row by comma and return as a dictionary
        columns = row.split(",")
        return { "column1": columns[0], "column2": columns[1], "column3": columns[2] , "column4": columns[3], "column5": columns[4], "column6": columns[5] }

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

# Usage example
project_id = "mstr-globalbi-sbx-c730"
dataset_id = "dev_lhannah_dataset_test"
table_id = "prd-lhannah_table_test"
csv_file_path = 'files/csv/flat.csv'
schemas_file_path = 'bqschemas/data_control.json'

csv_to_bigquery = CsvToBigQuery(project_id, dataset_id, table_id)
csv_to_bigquery.run(csv_file_path, schemas_file_path)