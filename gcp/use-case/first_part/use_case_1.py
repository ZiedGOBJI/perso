import json
import csv
from google.cloud import bigquery
from google.cloud import storage

def main(event, context):

    file_name = event['name']
    bucket_name = event['bucket']
    

    # connexion au storage pour récupérer le JSON
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_name)
    data = json.loads(blob.download_as_string())

    '''
    on va chercher toutes les clés de la base de données, ainsi que leur contenu
    data = [{row1},{row2},...]
    {row1}.keys() = ["datasetid","recordid","fields","record_timestamp"]
    la seule clé importante est "fields" car contient toutes les clés
    '''

    # on récupère toutes les clés contenues dans la clé "fields"
    field_keys = set()
    for _json in data:
        fields = _json.get("fields")
        if fields:
            field_keys |= set(fields.keys())
    columns = list(field_keys)
    print(f'columns: {columns}')

    # on récupère les données liées aux clés qu'on a récupéré précédemment 
    rows_to_insert = [{k: fields.get(k, None) for k in columns}
                    for _json in data
                    for fields in [_json.get("fields", {})]]
    print(f'rows_to_insert: {rows_to_insert}')


    # connexion à big query #
    bq_client = bigquery.Client()
    dataset_id = 'dataset_zied'
    table_id = 'last_table'
    
    schema = [bigquery.SchemaField(k, 'STRING') for k in columns]

    # Vérification si la table existe déjà
    table_ref = bq_client.dataset(dataset_id).table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = bq_client.create_table(table)

    # Insertion des données dans la table
    insertion = bq_client.insert_rows(table, rows_to_insert)

    print(f'Données insérées dans la table {table_id}.')
