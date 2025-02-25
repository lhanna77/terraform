###############################################################################################################
# Cloud function used to truncate all qa DWH tables. Part of drive to reduce costs.
# Source ticket - MNEXTV2-60652
##############################################################################################################

from google.cloud import bigquery

#target_project = 'mstr-sevenseas-prd-afea'
target_project = 'mstr-dwh-prd-4e3e'

# Construct a BigQuery client object.
client = bigquery.Client(project=target_project)

tables_to_truncate = [
# f'{target_project}.dl_pfp.billable_event',
# f'{target_project}.dl_pfp.campaign_event'
# f'{target_project}.dl_pfp.cap_event',
# f'{target_project}.dl_pfp.contract_event',
# f'{target_project}.dl_pfp.provider_event'

f'{target_project}.PFP.BillableEvent',
f'{target_project}.PFP.CampaignEventCurrent',
f'{target_project}.PFP.CampaignEventHistory',
f'{target_project}.PFP.CapEventCurrent',
f'{target_project}.PFP.CapEventHistory',
f'{target_project}.PFP.ContractEventCurrent',
f'{target_project}.PFP.ContractEventHistory',
f'{target_project}.PFP.ProviderEventCurrent',
f'{target_project}.PFP.ProviderEventHistory'

]

for table_name in tables_to_truncate:
    
    query_job = client.query(
        f"""
        
            SELECT
                count(*)
            FROM
                `{table_name}`

        
        """
    )

    tables_list = [f'{table_name} - ' + str(row[0]) for row in query_job]

    print(tables_list)