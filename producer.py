import json
from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob  # For searching for JSON file
import csv
import os

# Search the current directory for the JSON file (service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Set the project_id with your project ID
project_id = "firm-reef-449023-q6"
topic_name = "car-location-topic"  # Change if needed

# Create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Publishing messages to {topic_path}...")

# File path to the CSV file
csv_file_path = "Labels.csv"

# Read the CSV file and publish messages
with open(csv_file_path, "r") as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        # Convert the row (dict) to a JSON string
        message = json.dumps(row).encode("utf-8")  # Serialize

        # Publish the message
        print(f"Producing a record: {message}")
        future = publisher.publish(topic_path, message)

        # Ensure the publishing has completed successfully
        future.result()

print("All messages have been published.")
