from os import getlogin
from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client()

def data_control_backup(dataset_id,table_id):

    sql_query = f"""
        CREATE OR REPLACE TABLE
            `{dataset_id}.{table_id}_backup` 
        PARTITION BY DATE(RunDate) AS
        SELECT
            *
        FROM
            `{dataset_id}.{table_id}`
    """
    
    query_job = client.query(sql_query)
    #results = query_job.result()
    
    print(f'{dataset_id}.{table_id} backed up\n')

def get_table_list(dataset_id,table_id,condition_dataset_name,condition_table_name):

    sql_query = f"""
        SELECT
            dc.Dataset,
            dc.TableName,
            ARRAY_TO_STRING( ct.Name,',') AS ChildTable
        FROM
            `{dataset_id}.{table_id}` dc
        INNER JOIN
            UNNEST (ChildTable) ct
        WHERE
            Dataset = '{condition_dataset_name}'
        AND
            TableName = '{condition_table_name}'
    """

    query_job = client.query(sql_query)
    # Add main table entry first
    final_results_list = [f'{row[0]}.{row[1]}' for row in query_job] 
    # Append child tables (if any)
    final_results_list.extend(entry for row in query_job if row[2] for entry in row[2].split(','))
    
    return final_results_list

# def run_DAG():
    
#     import requests

#     # Replace 'your-project-id' with your GCP project ID
#     project_id = 'mstr-globalbi-dev-3a2f'

#     # Replace 'your-location' with the location of your Composer environment (e.g., 'us-central1')
#     location = 'us-east1'

#     # Replace 'your-composer-environment' with the name of your Composer environment
#     composer_environment = 'mstr-composer-dev-gbi-c2a2'

#     # Replace 'your-dag-id' with the ID of the DAG you want to trigger
#     dag_id = 'BI_zuora'

#     # The URL to trigger a DAG run in Google Cloud Composer
#     url = f"https://composer.googleapis.com/v1beta1/projects/{project_id}/locations/{location}/environments/{composer_environment}/dags/{dag_id}:trigger"

#     # Replace 'your-access-token' with a valid access token for your GCP project
#     headers = {
#         "Authorization": "Bearer ya29.a0AbVbY6M5esptz81jOcEJRIzzplWYUk976KKS_1LdSSDJQ5SqBusWVBkJH21ZdAtOk_jZx92qgQi-mFMHbITIpyLAI_hjbO_FRJ_vDe-DFvpPVbRiFb08hMhoGEBZP2orVJQ2dlTIvxzm5nF7zrccPFRQIfWq8Qt51whxaCgYKAYoSARASFQFWKvPl-wDCJ9LgDjRufie3j4SWmw0171",
#     }

#     # Trigger the DAG run
#     response = requests.post(url, headers=headers)

#     # Check the response status
#     if response.status_code == 200:
#         print(f"DAG {dag_id} has been triggered successfully.")
#     else:
#         print(f"Failed to trigger DAG {dag_id}. Status code: {response.status_code}")
        

def update_data_control(run_date, full_load, condition_dataset_name, condition_table_name, project = 'mstr-globalbi-sbx-c730', user_name = getlogin()):

    """
        Function to set dates for custom incremental load or flag for full pull. Full pull date will be set to - '1980-01-01 00:00:00 UTC'
        
        Parameters
        run_date - Date required for the incremental load, will be ignoreed for full pull 
        full_load - True or False.
        condition_dataset_name - Dataset of the table in DataControl to update. 
        condition_table_name - Table of the table in DataControl to update. 
        project = - Project where DataControl table is stored. 
    """

    dataset_id = f'{project}.dev_lhannah_dataset_test'
    table_id = 'dev_dev_ds_test'

    data_control_backup(dataset_id,table_id)
    final_results_list = get_table_list(dataset_id,table_id,condition_dataset_name,condition_table_name)

    for t in final_results_list:
        
        ds = t.split('.')[0]
        tbl = t.split('.')[1]

        if full_load == True:
            run_date = '1980-01-01 00:00:00 UTC'        

        update_values = {
            'RunDate': run_date,
            'FullLoad': full_load,
            'UpdatedBy': user_name,
            'UpdatedAt': str(datetime.now())
            
        }

        # Build the SQL UPDATE statement with appropriate data type conversions. val!r value in it's raw form, so string repr including quotes
        update_clause = ', '.join([f"{col} = {val!r}" if isinstance(val, str) else f"{col} = {val}" for col, val in update_values.items()])
        update_statement = f"""
            UPDATE `{dataset_id}.{table_id}`
            SET {update_clause}
            WHERE Dataset = '{ds}' AND TableName = '{tbl}'
        """
        
        query_job = client.query(update_statement)
        #results = query_job.result()
        
        print(f'{t} row in {dataset_id}.{table_id} updated to full_load:{full_load} / run_date:{run_date}, updated by {user_name}')

def get_project_selection():
    
    dev = 'mstr-globalbi-dev-3a2f'
    qa = 'mstr-globalbi-qa-5e9b'
    prd = 'mstr-globalbi-prd-7761'
    
    print("Select a project:")
    print(f"1. {dev}")
    print(f"2. {qa}")
    print(f"3. {prd}")

    user_input = input("Enter the option number (1/2/3): ")

    if user_input == '1':
        selected_option = f"{dev}"
    elif user_input == '2':
        selected_option = f"{qa}"
    elif user_input == '3':
        selected_option = f"{prd}"
    else:
        print("\nInvalid option. Please enter a valid option number.")
        return get_project_selection()

    return selected_option

if __name__ == '__main__':
    
    selected_option = get_project_selection()
    print(f"\nYou selected: {selected_option}!\n")
    update_data_control('2023-01-01 00:00:00 UTC', False, 'GoogleAnalytics', 'GoogleAnalyticsSeekerProfileCreation')    #, selected_option


# run_date = '2023-07-31 00:00:00 UTC'
# full_load = False
# condition_dataset_name = 'GoogleAnalytics'
# condition_table_name = 'GoogleAnalyticsSeekerProfileCreation'