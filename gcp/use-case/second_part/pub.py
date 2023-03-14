import os
from google.cloud import pubsub_v1
from google.cloud import storage
import json
import base64

def pub(event, context):

    publisher = pubsub_v1.PublisherClient()
    storage_client = storage.Client()
    topic_name = os.getenv("TOPIC_NAME")
    
    blob = storage_client.bucket(event["bucket"]).blob(event["name"])
    data = blob.download_as_text().split('\n')

    encoded_message = json.dumps(data).encode('utf-8')
    response = publisher.publish(topic_name, data=encoded_message)
    
    return response.result()