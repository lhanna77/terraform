from google.cloud import bigquery
from pathlib import Path

#from lib.upload_to_storage_bucket import upload_to_storage_bucket
from lib.write_data_bigquery import write_data_bigquery
from lib.dataflow_jsonl_bigquery import dataflow_jsonl_bigquery

def main():

    default_bucket = 'lhannah-sink-dev'

    tables_to_populate = [
    # {'file_format':'json','file_name':'sfmc','bucket':default_bucket,'table_id':'dev_sfmc'},
    # {'file_format':'json','file_name':'sfmc-struct','bucket':default_bucket,'table_id':'dev_sfmc_struct'},
    # {'file_format':'json','file_name':'nested','bucket':default_bucket,'table_id':'dev_schema_test'},
    # {'file_format':'json','file_name':'flat','bucket':default_bucket,'table_id':'prd_lhannah_table_test'},
    # {'file_format':'json','file_name':'query1','bucket':default_bucket,'table_id':'dev_query1'},
    {'file_format':'json','file_name':'JobAppliesCurrentV2','bucket':default_bucket,'table_id':'dev_JobAppliesCurrent'}
    ]

    for t in tables_to_populate:
        dict_file_format = t["file_format"]
        dict_file_name = t["file_name"]
        dict_bucket = t["bucket"]
        dict_table_id = t["table_id"]

        file_name = f'{dict_file_name}.{dict_file_format}'
        tmp_file_path = Path(fr'files\{dict_file_format}\{file_name}')

        if dict_file_format == 'csv':
            source_format = bigquery.SourceFormat.CSV
        else:
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

        #blob_id = upload_to_storage_bucket(file_name, tmp_file_path, dict_bucket)

        #write_data_bigquery(dict_table_id, f"gs://{dict_bucket}/{blob_id.name}", source_format)
        
        dataflow_jsonl_bigquery(dict_file_format, dict_file_name, dict_bucket, dict_table_id)
    
if __name__ == '__main__':
    main()