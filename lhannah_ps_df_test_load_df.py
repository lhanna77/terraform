#!/usr/bin/env python3.9
import json
import logging
import apache_beam as beam
from argparse import ArgumentParser
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

BQ_TABLE = "dev_LhannahPsDfTest"
SCHEMA_PATH = "bqschemas/lhannah_ps_df_test.json"
FROM_FILE_PATH = "files/json/DataControl.json"
SOURCE_IS_FILE = "False"
SUBSCRIPTION_ID = "lhannah-ps-df-test-sub"

# Service account key path
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/e62e3e6ed7d7.json"
INPUT_SUBSCRIPTION = f"projects/mstr-globalbi-sbx-c730/subscriptions/"
BIGQUERY_DATASET = f"mstr-globalbi-sbx-c730:dev_lhannah_dataset_test."
#BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"

def get_table_schema(schema_path=SCHEMA_PATH):
    
    #JSON_DATA = json.load(open(SCHEMA))
    schema_data = json.dumps(json.load(open(schema_path)))
    return parse_table_schema_from_json(schema_data)

class CustomParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation """

    def to_runner_api_parameter(self, unused_context):
        # Not very relevant, returns a URN (uniform resource name) and the payload
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: bytes, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """
        Simple processing function to parse the data and add a timestamp
        For additional params see:
        https://beam.apache.org/releases/pydoc/2.7.0/apache_beam.transforms.core.html#apache_beam.transforms.core.DoFn
        """
        parsed = json.loads(element.decode("utf-8"))
        # parsed["UpdatedBy"] = "lee.hannah@monster.co.uk"
        # parsed["UpdatedAt"] = timestamp.to_rfc3339() #timestamp / UpdatedAt
        yield parsed


def run(pipeline_args, input_subscription = INPUT_SUBSCRIPTION+SUBSCRIPTION_ID, bq_table = BIGQUERY_DATASET+BQ_TABLE, bigquery_schema = SCHEMA_PATH):

    print(input_subscription)
    print(bq_table)
    print(bigquery_schema)

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    # Defining our pipeline and its steps
    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=input_subscription, timestamp_attribute=None
            )
            | "CustomParse" >> beam.ParDo(CustomParsing())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=bq_table,
                schema=get_table_schema(bigquery_schema),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                #method="STREAMING_INSERTS"
            )
        )

def run_from_file(pipeline_args, from_file_path = FROM_FILE_PATH, bq_table = BIGQUERY_DATASET+BQ_TABLE, bigquery_schema = SCHEMA_PATH):

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args, temp_location = "gs://lhannah-sink-dev/df")

    # Defining our pipeline and its steps
    pipeline = beam.Pipeline(options=pipeline_options)
    
    (
        pipeline
        | "Read JSONL" >> beam.io.ReadFromText(from_file_path)
        | "CustomParse" >> beam.Map(parse_jsonl_row)
        | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            table=bq_table,
            schema=get_table_schema(bigquery_schema),
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        )
    )
    
    pipeline.run().wait_until_finish()

    print("\n")
    print(f"Load complete")
    print("\n")
    
def parse_jsonl_row(row):
    # Parse JSONL row and return as a dictionary
    json_data = json.loads(row)
    
    print(json_data)
    
    return json_data

if __name__ == "__main__":
    
    # Parsing arguments
    parser = ArgumentParser()
    parser.add_argument(
        '-sif', '--source_is_file', help='Whether topic populated from file.', default=SOURCE_IS_FILE,
    )
    parser.add_argument(
        '-fp', '--from_file_path', help='Path to file to populate table.', default=FROM_FILE_PATH
    )
    parser.add_argument(
        '-sub', '--input_subscription',
        help='Input PubSub subscription of the form projects/<PROJECT>/subscriptions/<SUBSCRIPTION>.',
        default=INPUT_SUBSCRIPTION+SUBSCRIPTION_ID,
    )
    parser.add_argument(
        '-t', '--table_output', help='Output BigQuery Table', default=BIGQUERY_DATASET+BQ_TABLE
    )
    parser.add_argument(
        '-s', '--schema_output', help='Output BigQuery Schema in text format', default=SCHEMA_PATH,
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    
    if vars(known_args)['source_is_file'] == "False":
        run(pipeline_args, INPUT_SUBSCRIPTION+vars(known_args)['input_subscription'], BIGQUERY_DATASET+vars(known_args)['table_output'], vars(known_args)['schema_output'])
    else:
        run_from_file(pipeline_args, vars(known_args)['from_file_path'],  BIGQUERY_DATASET+vars(known_args)['table_output'], vars(known_args)['schema_output'])

# py -3.9 lhannah_ps_df_test_load_df.py --streaming \
# --runner DataflowRunner \
# --project mstr-globalbi-sbx-c730 \
# --region us-east1 \
# --temp_location gs://lhannah-sink-dev/df/ \
# --job_name lhannah-ps-df-test-dataflow \
# --max_num_workers 2 \
# --input_subscription projects/mstr-globalbi-sbx-c730/subscriptions/lhannah-ps-df-test-sub-file \
# --output_table mstr-globalbi-sbx-c730:dev_lhannah_dataset_test.dev_DataControl \
# --output_schema parse_table_schema_from_json("data_control_pub_sub")

# py -3.9 -m lhannah_ps_df_test_load_df --input_subscription projects/mstr-globalbi-sbx-c730/subscriptions/lhannah-ps-df-test-sub \
# --output_table mstr-globalbi-sbx-c730:dev_lhannah_dataset_test.dev_LhannahPsDfTest \
# --output_schema parse_table_schema_from_json("lhannah_ps_df_test.json")