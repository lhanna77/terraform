#Data Extensions >NEXT >Journeys >Data Extracts >Datalake
#datalake_extract_smsmessagetracking = 540 records Seeker QA
#https://developer.salesforce.com/docs/marketing/marketing-cloud/references/mc-custom_object_data

import requests
import pandas as pd
import time

security_token = 'gDCJ5nLxtPNtCUu9kXbT8un4X'
client_key = '3MVG9zJJ_hX_0bb_vLVlHZgsOTx199LDLRjK.o6xTMTFmCaAuWHzRR0nHcvdl36lYIpyjrv.owbrSyYYA0TS8'
client_secret = '33414E5D604C1AB2EE41B974BFE2453D1979946A90A81C4E97DAFAC9E9677E8E'
domain_name = 'https://mforce--mforceqa1.sandbox.my.salesforce.com/'
endpoint = 'services/data/v61.0/sobjects/Account/'

#endpoint = 'services/data/v61.0/query?q=SELECT Id FROM Account'

endpoint = 'services/data/'

def get_access_token():

    # Define the authentication endpoint URL
    auth_url = auth_url = domain_name + 'services/oauth2/token'
    
    # Define the authentication payload
    payload = {
        'grant_type': 'password', # client_credentials / password
        'client_id': client_key,
        'client_secret': client_secret,
        'username': 'lee.hannah@monster.co.uk.qa1'
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

# Initialize an empty DataFrame to store the results
all_records = pd.DataFrame()

# Define the access token
#access_token = get_access_token()
access_token = '6Cel800Df0000003dFRN888ct0000000AgvYULRgGtiFRE4kTYsFN9JVAv9mLQhbaYeoe05QGsN73TiASEkQrKVMSoHFy9rqhpTRRhHbT50'

# Construct the complete URL
url = domain_name + endpoint

# Define the request headers with the access token
headers = {
    'Authorization': 'Bearer ' + access_token,
    'Content-Type': 'application/json'
    'X-PrettyPrint:1'
}

# Send the GET request to retrieve data from the Data Extension
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Extract the data from the response
    data = response.json()
    print("Data:", data)
    
else:
    print("Failed to retrieve data. Status code:", response.status_code)
    print("Error message:", response.text)