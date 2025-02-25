from google.cloud import bigquery
import csv
import os
from datetime import datetime

os.environ["GCLOUD_PROJECT"] = "mstr-dwh-dev-83bf"

def run_query_and_save_to_csv(query, env, table):
    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Run the query
    query_job = client.query(query)
    
    # Get query results
    results = query_job.result()
    
    # Generate a timestamp for the CSV file name
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Define the file path
    downloads_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    csv_file_path = os.path.join(downloads_folder, f"{env}_{table}_{timestamp}.csv")
    
    # Write results to CSV file
    with open(csv_file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write headers
        writer.writerow([field.name for field in results.schema])
        
        # Write rows
        for row in results:
            writer.writerow(row)
    
    return csv_file_path

def main():

    # Replace with your GCP project ID, dataset ID, and table ID
    project_id = ['mstr-sevenseas-qa-4d5b'] # 'mstr-sevenseas-dev-cd2c',
    pfp_tables = ['billable_event'] # campaign_event','cap_event','contract_event','provider_event
    
    # DWH
    # project_id = ['mstr-dwh-dev-83bf'] 
    # pfp_tables = ['ContractEventHistory','ContractEventCurrent']
    
    for p in project_id:
        for pt in pfp_tables:
        
            env = p.split('-')[2]
            
            dataset_id_pfp = 'dl_pfp'
            #dataset_id_pfp = 'PFP'

            # Job applies query
            query_pfp = f"""
                SELECT 
                    dataLake.attributes.conversionSchemaPath,billableEvent.* 
                FROM
                    `{p}.{dataset_id_pfp}.{pt}` pfp WHERE TIMESTAMP_TRUNC(IFNULL(_PARTITIONTIME, current_timestamp()), DAY) >= TIMESTAMP("2024-08-25");
            """

            # Run the query and save results to CSV
            csv_file_path = run_query_and_save_to_csv(query_pfp, env, pt)
            print(f"{env} {pt} CSV file saved to: {csv_file_path}")

if __name__ == "__main__":
    main()