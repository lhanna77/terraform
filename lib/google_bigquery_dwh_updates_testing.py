from google.cloud import bigquery
import csv
import os
from datetime import datetime

os.environ["GCLOUD_PROJECT"] = "mstr-dwh-dev-83bf"

def run_query_and_save_to_csv(query, env, dataset_id):
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
    csv_file_path = os.path.join(downloads_folder, f"{env}_dwh_{dataset_id}_{timestamp}.csv")
    
    # Write results to CSV file
    with open(csv_file_path, 'w', encoding="utf-8", newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write headers
        writer.writerow([field.name for field in results.schema])
        
        # Write rows
        for row in results:
            writer.writerow(row)
    
    return csv_file_path

def main():

    # Replace with your GCP project ID, dataset ID, and table ID
    project_id = ['mstr-dwh-dev-83bf','mstr-dwh-qa-0ab6']
    
    for p in project_id:
        
        env = p.split('-')[2]
        
        dataset_id_jobapplies = 'JobApplies'
        table_id_jobapplies = 'JobAppliesHistory'

        # Job applies query
        query_jobapplies = f"""
            SELECT 
                JobApplyID,
                dataLake.attributes.conversionSchemaPath,
                serverEvent.traceId,
                serverEvent.eventType,
                serverEvent.entityId,
                TIMESTAMP_TRUNC(IFNULL(SourceETLDate,CURRENT_TIMESTAMP()), DAY) AS ingestionDate,
                user.firstName,
                user.lastName,
                Quarantined,
                ApplyNote,
                Stage
            FROM
                `{p}.{dataset_id_jobapplies}.{table_id_jobapplies}`
            WHERE
                TIMESTAMP_TRUNC(IFNULL(SourceETLDate,CURRENT_TIMESTAMP()), DAY) >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
            AND     
                dataLake.attributes.conversionSchemaPath = 'job-applies/apply/v2.0.5/apply.avcs.json'
            ORDER BY
                ingestionDate;
        """

        # Run the query and save results to CSV
        csv_file_path = run_query_and_save_to_csv(query_jobapplies, env , dataset_id_jobapplies)
        print(f"{env} {dataset_id_jobapplies} CSV file saved to: {csv_file_path}")

        dataset_id_profiles = 'Profiles'
        table_id_profiles = 'ProfilesHistory'

        # Job applies query
        query_profiles = f"""
            SELECT 
                ProfileID,
                dataLake.attributes.conversionSchemaPath,
                serverEvent.traceId,
                serverEvent.eventType,
                serverEvent.entityId,
                TIMESTAMP_TRUNC(IFNULL(SourceETLDate,CURRENT_TIMESTAMP()), DAY) AS ingestionDate,
                Quarantine
            FROM
                `{p}.{dataset_id_profiles}.{table_id_profiles}`
            WHERE
                TIMESTAMP_TRUNC(IFNULL(SourceETLDate,CURRENT_TIMESTAMP()), DAY) >= DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
            AND 
                dataLake.attributes.conversionSchemaPath = 'profiles/profile/v2.0.9/profile.avcs.json'
            ORDER BY
                ingestionDate;
        """

        # Run the query and save results to CSV
        csv_file_path = run_query_and_save_to_csv(query_profiles, env, dataset_id_profiles)
        print(f"{env} {dataset_id_profiles} CSV file saved to: {csv_file_path}")

if __name__ == "__main__":
    main()