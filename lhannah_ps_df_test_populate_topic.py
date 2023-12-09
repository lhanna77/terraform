# requirements
# google-cloud-pubsub==1.7.0 
import json
import time
from argparse import ArgumentParser
from datetime import datetime
from random import random,choice
from google.auth import jwt
from google.cloud import pubsub_v1

# --- Base variables and auth path
#CREDENTIALS_PATH = "credentials/e62e3e6ed7d7.json"
PROJECT_ID = "mstr-globalbi-sbx-c730"
TOPIC_ID = "lhannah-ps-df-test-topic"
IMPORT_FILE = "files/json/DataControl.json"
FROM_FILE_PATH = "False"
MAX_MESSAGES = 25
FOOD = ["apple", "banana", "grape", "pear", "mellon", "blueberry"]

# --- PubSub Utils Classes
class PubSubPublisher:
    def __init__(self, project_id, topic_id):
        # credentials = jwt.Credentials.from_service_account_info(
        #     json.load(open(credentials_path)),
        #     audience="https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        # )
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    def publish(self, data: str):
        result = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        return result


# --- Main publishing script
def main(topic_to_populate = TOPIC_ID, from_file = "False", file_path = IMPORT_FILE):
    
    publisher = PubSubPublisher(PROJECT_ID, topic_to_populate)
    
    if from_file == "False":
    
        i = 0
        
        while i < MAX_MESSAGES:
            data = {
                "attr1": random(),
                "msg": f"Msg {i} - Hi, the time is now -{datetime.now()}, so time for a {choice(FOOD)}"
            }
            
            publisher.publish(json.dumps(data))
            print(json.dumps(data))
            time.sleep(random())
            i += 1
            
        print("\n")
        print(f"Published to - {topic_to_populate} from code")
        print("\n")
            
    else:
        
        jsonl_records = get_records_from_file(file_path)
        
        # Access each record in the list
        for data in jsonl_records:
            
            publisher.publish(json.dumps(data))
            print(json.dumps(data))
            time.sleep(random())
            
        print("\n")
        print(f"Published to - {topic_to_populate} from {file_path}")
        print("\n")
        
def get_records_from_file(file_path):
    
    jsonl_records = []
    with open(file_path, 'r') as file:
        for line in file:
            record = json.loads(line)
            jsonl_records.append(record)
    return jsonl_records


if __name__ == "__main__":
    
    # Parsing arguments
    parser = ArgumentParser()

    parser.add_argument(
        '-t', '--to_topic', help='Send the records to this topic.', default=TOPIC_ID,
    )
    parser.add_argument(
        '-sif', '--source_is_file', help='Whether topic populated from file.', default=FROM_FILE_PATH,
    )
    parser.add_argument(
        '-fp', '--from_file_path', help='Path to file to populate topic.', default=IMPORT_FILE,
    )
    
    known_args, pipeline_args = parser.parse_known_args()
    
    if vars(known_args)['source_is_file'] == "False":
        main(from_file="False")
    else:
        main(topic_to_populate=vars(known_args)['to_topic'],from_file=True,file_path=vars(known_args)['from_file_path'])
        
#py -3.9 lhannah_ps_df_test_populate_topic.py
#py -3.9 lhannah_ps_df_test_populate_topic.py -t lhannah-ps-df-test-topic-file -sif "True" --fp files/json/DataControl.json