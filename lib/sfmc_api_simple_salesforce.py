from simple_salesforce import Salesforce, SalesforceLogin, SalesforceMalformedRequest
import pandas as pd
from datetime import date, timedelta
import logging
import os
from upload_to_storage_bucket import upload_to_storage_bucket
from create_read_config import create_config, read_config

create_config()
config_dict = read_config()

salesforce_username = config_dict['salesforce_username']
salesforce_password = config_dict['salesforce_password']
salesforce_security_token = config_dict['salesforce_security_token']
salesforce_domain = config_dict['salesforce_domain']

date_today = date.today().strftime('%Y-%m-%dT%H:%M:%SZ')
date_yest = date.today() - timedelta(days=1)
date_yest = date_yest.strftime('%Y-%m-%dT%H:%M:%SZ')

PARTITION_DATE = "LastModifiedDate"

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sf_tables_dict_list = [ 
    {"table_name":"Account",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, Name, LastName, FirstName, Salutation, MiddleName, Suffix, Type, CreatedDate, LastModifiedDate"},
    {"table_name":"AccountContactRelation",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"Contact",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, IsPersonAccount, CreatedDate, LastModifiedDate"},
    {"table_name":"Event",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"EventWhoRelation",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"Lead",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"Opportunity",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"RecordType",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, CreatedDate, LastModifiedDate"},
    {"table_name":"Task",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"TaskWhoRelation",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, IsDeleted, CreatedDate, LastModifiedDate"},
    {"table_name":"User",
    "partition_date":PARTITION_DATE,
    "table_cols":"Id, CreatedDate, LastModifiedDate"}
    
]

# Salesforce login
try:
    session, instance = SalesforceLogin(username=salesforce_username, password=salesforce_password, security_token=salesforce_security_token, domain=salesforce_domain)
    sf = Salesforce(instance=instance, session_id=session)
    logging.info('Salesforce login successful')
except SalesforceMalformedRequest as e:
    logging.error(f'Salesforce login failed: {e.content}')
    raise
except Exception as e:
    logging.error(f'An error occurred during Salesforce login: {str(e)}')
    raise

# Querying and file handling
for table in sf_tables_dict_list:
    try:
        query_to_run = f'SELECT {table["table_cols"]} FROM {table["table_name"]} WHERE {table["partition_date"]} >= {date_yest} AND {table["partition_date"]} < {date_today}'
        logging.info(f'Running query for table: {table["table_name"]}')
       
        # Execute the query
        query_results = sf.query_all(query_to_run)
        logging.info(f'{table["table_name"]} totalSize - {query_results["totalSize"]}')
       
        # Proceed if results exist
        if query_results["totalSize"] > 0:
            df = pd.DataFrame(query_results['records']).drop(columns='attributes')
           
            # Construct the output file path
            output_file = os.path.join(
                'C:/Users/lehannah/OneDrive - Monster_AD/Monster Files/Learning/Terraform/Udemy/terraform/files/salesforce',
                f'{table["table_name"]}.jsonl'
            )
           
            # Write DataFrame to JSONL
            df.to_json(output_file, orient='records', lines=True, force_ascii=False)
            logging.info(f'File {output_file} populated from API')
           
            # Upload to storage bucket
            upload_to_storage_bucket(f'salesforce_{table["table_name"]}', output_file, 'lh-bck-salesforce-test')
        else:
            logging.info(f'Table {table["table_name"]} has no changes from yesterday')
    except Exception as e:
        logging.error(f'Error processing table {table["table_name"]}: {str(e)}')