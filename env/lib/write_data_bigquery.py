from google.cloud import bigquery

def write_data_bigquery(table_id, uri, source_format):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = table_id

    job_config = bigquery.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("Project", "STRING"),
        #     bigquery.SchemaField("Dataset", "STRING"),
        #     bigquery.SchemaField("TableName", "STRING"),
        #     bigquery.SchemaField("RunDate", "TIMESTAMP"),
        #     bigquery.SchemaField("ETLDate", "TIMESTAMP"),
        #     bigquery.SchemaField("FullLoad", "BOOLEAN"),
        # ],
        #autodetect=True,
        source_format=source_format,
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
    print(f"Loaded {destination_table.num_rows} rows.")