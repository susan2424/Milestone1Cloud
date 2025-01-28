from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # Enables the JSON file search
import json
import os

# Browses the Design directory for the JSON file 
# sets the nessesary GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Setting car-location-topic as the topic name and firm-reef-449023-q6 JSON file as the project ID
project_id = "firm-reef-449023-q6"
topic_name = "car-location-topic"  # Change if needed
subscription_id = "car-location-topic-sub"  # Change if needed

# Create a publisher and get the topic path for it 
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

print(f"Listening for messages on {subscription_path}...\n")

# The function for getting the message
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    # Deserialize the message data (bytes to string, then JSON)
    message_data = json.loads(message.data.decode("utf-8"))
    print("Consumed record with value:", message_data)

    # The acknowledgement message to Google Pub/Sub
    message.ack()

with subscriber:
    # Allows to for subscription and listetning to message from the topic
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        print("\nStopped listening for messages.")
