import json
import time
import random
from google.cloud import pubsub_v1
import pandas as pd

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Project and Topic details
project_id = "hive-project-new-412916"
topic_name = "Inventory_data"
topic_path = publisher.topic_path(project_id, topic_name)

# Callback function to handle the publishing results.
def callback(future):
    try:
        # Get the message_id after publishing.
        message_id = future.result()
        print(f"Published message with ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message: {e}")

df = pd.read_csv('inventory_data.csv') 
df = df.fillna('null')    
print("Inventory data file read successfully")

for index, row in df.iterrows():
    # Create a dictionary from the row values
    value = row.to_dict()
    json_data=json.dumps(value).encode('utf-8')

    try:
        future = publisher.publish(topic_path, data=json_data)
        future.add_done_callback(callback)
        future.result()
    except Exception as e:
        print(f"Exception encountered: {e}")

    time.sleep(1)

print("All Inventory Data successfully published to Kafka")