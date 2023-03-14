import os
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import storage
import json
import base64


def sub(event, context):
 
    bigquery_client = bigquery.Client()
    dataset_name = os.getenv("DATASET_NAME")
    table_name = os.getenv("TABLE_NAME")

    dataset = bigquery_client.get_dataset(dataset_name)
    table = dataset.table(table_name)

    data = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(data)

    result = bigquery_client.load_table_from_json(data, table)

    return result.state
