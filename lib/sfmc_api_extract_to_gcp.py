#Data Extensions >NEXT >Journeys >Data Extracts >Datalake
#datalake_extract_smsmessagetracking = 540 records Seeker QA
#https://developer.salesforce.com/docs/marketing/marketing-cloud/references/mc-custom_object_data

import requests
import pandas as pd
import time

def get_access_token():

    # Define the authentication endpoint URL
    auth_url = 'https://mck5ms8gsgvyx4zn98fqbpwz54p0.auth.marketingcloudapis.com/v2/token'

    # Define the authentication payload
    payload = {
        'grant_type': 'client_credentials',
        'client_id': 'q2en73zkvmoo9tqhhrcft5bm',
        'client_secret': 'CkB6vSaOibV1IaArbpsKknSU',
        'account_id': '110005089'
        #Instance 11: https://mc.s11.exacttarget.com/
    }

    # Make a POST request to authenticate and obtain an access token
    response = requests.post(auth_url, json=payload)

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the access token from the response
        access_token = response.json()['access_token']
        print("Access token:", access_token)
        
        return access_token
    else:
        print("Failed to authenticate. Status code:", response.status_code)
        print("Error message:", response.text)

start_time = time.time()

print(f'start_time - {start_time}')

data_ext = 'datalake_extract_bounce'

# Define pagination parameters
page_number = 1 # Start with the first page

# Initialize an empty DataFrame to store the results
all_records = pd.DataFrame()

# Define the base URL for the SFMC API
base_url = 'https://mck5ms8gsgvyx4zn98fqbpwz54p0.rest.marketingcloudapis.com/'

# Define the access token
access_token = get_access_token()

# Loop through pages until all data is retrieved
while True:
    
    items_list = []

    # Define the endpoint for retrieving data from a Data Extension
    endpoint = f'data/v1/customobjectdata/key/{data_ext}/rowset?$page={page_number}' #$filter=abandonedApplyDate%20gt%20"4/28/2024 22:00:00 PM"&
    #endpoint = f'hub/v1/dataevents/key:{data_ext}/rowset'
    #endpoint = f'asset/v1/content/assets/'

    # Construct the complete URL
    url = base_url + endpoint

    # Define the request headers with the access token
    headers = {
        'Authorization': 'Bearer ' + access_token,
        'Content-Type': 'application/json'
    }

    # Send the GET request to retrieve data from the Data Extension
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the data from the response
        data = response.json()
        #print("Data:", data)
        
        # Check if there are no more records
        if data['count']==len(all_records) or data['count']==0:
            end_time = time.time()
            print(f'end_time - {end_time}')
            execution_time = end_time-start_time
            print(f'Final count = {len(all_records)} ... completed in {execution_time:.2f}')
            break
            
        for i in data['items']:
            # Append the page records to the DataFrame
            #all_records = all_records.append(i['values'], ignore_index=True)
            
            df_new_row = pd.DataFrame(i['values'], index=[0])
            all_records = pd.concat([all_records,df_new_row], ignore_index=True)
            
            print(len(all_records))

        # Move to the next page
        page_number += 1
        
        
    else:
        print("Failed to retrieve data. Status code:", response.status_code)
        print("Error message:", response.text)