# Snooze low traffic Pub/Sub topic alerts over the weekend

import datetime
import os
from google.cloud import monitoring_v3
from google.protobuf import timestamp_pb2

def get_alert_policies(project_name: str) -> list:
    """List alert policies in a project.

    Arguments:
        project_name (str): The Google Cloud Project to use. 
        The project name must be in the format - 'projects/<PROJECT_NAME>'.
    """

    client = monitoring_v3.AlertPolicyServiceClient()
    policies = client.list_alert_policies(name=project_name)

    alert_policy_id = [policy.name for policy in policies if policy.display_name == 'Pub/Sub topic no messages in 24 hours (low traffic topic)']
    
    return alert_policy_id

def create_weekend_snooze(project_name: str, alert_policy_id: list, snooze_name: str) -> str:
    
    """Create alert policies.

    Arguments:
        project_name (str): The Google Cloud Project to use. The project name must be in the format - 'projects/<PROJECT_NAME>'.
        alert_policy_id (list): 1 item, the Google Cloud Policy to snooze.
        snooze_name (str): What to name the new snooze.
    """
    
    # Create a client
    client = monitoring_v3.SnoozeServiceClient()

    # Get current date and calculate the next Saturday and Monday
    today = datetime.datetime.today()
    saturday = today + datetime.timedelta((5 - today.weekday()) % 7)  # Next Saturday
    monday = saturday + datetime.timedelta(days=2)  # Next Monday

    # Set start and end times for the weekend
    start_time = datetime.datetime.combine(saturday, datetime.time.min)  # Start at 00:00 Saturday
    set_time_monday = datetime.datetime.strptime('1159','%H%M').time()
    end_time = datetime.datetime.combine(monday, set_time_monday)  # End at 11:59 Monday

    # Convert to protobuf timestamps
    start_timestamp = timestamp_pb2.Timestamp()
    start_timestamp.FromDatetime(start_time)

    end_timestamp = timestamp_pb2.Timestamp()
    end_timestamp.FromDatetime(end_time)

    # Initialize request argument(s)
    snooze = monitoring_v3.Snooze(
        criteria = monitoring_v3.Snooze.Criteria(
            policies = alert_policy_id
        ),
        interval=monitoring_v3.TimeInterval(
            start_time = start_timestamp,
            end_time = end_timestamp,
        ),
    )
    
    #snooze.name = snooze_name
    snooze.display_name = snooze_name

    request = monitoring_v3.CreateSnoozeRequest(
        parent = project_name,
        snooze = snooze,
    )

    # Make the request
    response = client.create_snooze(request=request)

    response_txt = f"Weekend snooze created: {response.name}. Active from {start_time} to {end_time}"
    
    return response_txt

def main():
    project_id = 'mstr-datastage-dev-df70' #os.environ['PROJECT_ID']
    project_name = f'projects/{project_id}'
    alert_policy_id = get_alert_policies(project_name)
    snooze_name = f"snooze weekend low traffic - {datetime.date.today().strftime('%Y/%m/%d')}"

    response_txt = create_weekend_snooze(project_name, alert_policy_id, snooze_name)
    
    print(response_txt)
    
    return response_txt

if __name__ == '__main__':
	main()