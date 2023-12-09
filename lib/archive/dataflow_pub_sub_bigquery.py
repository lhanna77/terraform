import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json

class PubsubToBigQuery:
    def __init__(self, project_id, dataset_id, table_id, schema_file_path):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.schema_file_path = schema_file_path

    def run(self, input_topic):
        pipeline_options = PipelineOptions()
        pipeline = beam.Pipeline(options=pipeline_options)

        (
            pipeline
            | "Read Pub/Sub" >> beam.io.ReadFromPubSub(topic=input_topic)
            | "Parse Pub/Sub Message" >> beam.Map(self.parse_pubsub_message)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                self.table_id,
                dataset=self.dataset_id,
                project=self.project_id,
                schema=self.get_table_schema(),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        pipeline.run().wait_until_finish()

    def parse_pubsub_message(self, message):
        # Parse Pub/Sub message and return as a dictionary
        message_data = message.decode("utf-8")
        return json.loads(message_data)

    def get_table_schema(self):
        # Load the JSON schema from file
        with open(self.schema_file_path) as schema_file:
            schema_json = json.load(schema_file)

        # Define the BigQuery table schema
        schema = bigquery.TableSchema()
        for field in schema_json:
            schema_field = bigquery.TableFieldSchema()
            schema_field.name = field['name']
            schema_field.type = field['type']
            schema_field.mode = field['mode']
            schema.fields.append(schema_field)

        return schema

# Usage example
project_id = "your-project-id"
dataset_id = "your-dataset-id"
table_id = "your-table-id"
input_topic = "projects/your-project/topics/your-topic"
schema_file_path = "path/to/your/schema.json"

pubsub_to_bigquery = PubsubToBigQuery(project_id, dataset_id, table_id, schema_file_path)
pubsub_to_bigquery.run(input_topic)