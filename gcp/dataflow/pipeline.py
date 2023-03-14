from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from google.cloud import bigquery

import apache_beam as beam
import json
from config import PROJECT_ID, BUCKET, INPUT_NAF, INPUT_CONSO, OUTPUT_CONSO_BUCKET, TABLE_CONSO, TABLE_JOIN, STAGING_LOC, TMP_LOC, REGION, JOB_NAME, RUNNER

# Schéma pour les données NAF
schema_naf =  {'fields': [
        {'name': 'ligne', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_naf', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'int_v2_40c', 'type': 'STRING', 'mode': 'NULLABLE'}]}

keys_naf = ["ligne", "code_naf", "int_v2_40c"]

# Schéma pour les données de consommation

schema_conso = {'fields': [
        {'name': 'operateur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'annee', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'filiere', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_categorie_consommation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_categorie_consommation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_grand_secteur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_grand_secteur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_naf', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_secteur_naf2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'conso', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'pdl', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'indqual', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nombre_mailles_secretisees', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_region', 'type': 'STRING', 'mode': 'NULLABLE'}]}

keys_conso = ["operateur","annee","filiere","code_categorie_consommation","libelle_categorie_consommation","code_grand_secteur","libelle_grand_secteur","code_naf","libelle_secteur_naf2","conso","pdl","indqual","nombre_mailles_secretisees","code_region","libelle_region"]

# Schéma pour les données jointes
schema_join = {'fields': [
        {'name': 'operateur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'annee', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'filiere', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_categorie_consommation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_categorie_consommation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_grand_secteur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_grand_secteur', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_naf', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_secteur_naf2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'conso', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'pdl', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'indqual', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'nombre_mailles_secretisees', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'code_region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'libelle_region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ligne', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'int_v2_40c', 'type': 'STRING', 'mode': 'NULLABLE'}]}








def naf_to_json(input: str):
    row = input.split(';')
    yield {
        'ligne': row[0],
        'code_naf': row[1],
        'int_v2_40c': row[2],
    }

def conso_to_json(input:str):
    row = input.split(';')
    yield {
        'operateur': row[0],
        'annee': row[1],
        'filiere': row[2],
        'code_categorie_consommation': row[3],
        'libelle_categorie_consommation': row[4],
        'code_grand_secteur': row[5],
        'libelle_grand_secteur': row[6],
        'code_naf': row[7],
        'libelle_secteur_naf2': row[8],
        'conso': row[9],
        'pdl': row[10],
        'indqual': row[11],
        'nombre_mailles_secretisees': row[12],
        'code_region': row[13],
        'libelle_region': row[14],
    }  

def conso_join_naf(element):
    code_naf = element[0]
    conso_dicts = element[1]['conso']
    naf_dicts = element[1]['naf']
    for conso_dict in conso_dicts:
        for naf_dict in naf_dicts:
            if code_naf == naf_dict.get("code_naf"):
                jointure = {}
                for key in conso_dict:
                    jointure[key] = conso_dict[key]
                for key in naf_dict:
                    if key != "code_naf":
                        jointure[key] = naf_dict[key]
                yield jointure








# Define the pipeline options
options = PipelineOptions(
    runner= RUNNER ,
    project= PROJECT_ID,
    job_name= JOB_NAME,
    staging_location = STAGING_LOC,
    temp_location = TMP_LOC,
    region = REGION,
    max_num_workers=1)

with beam.Pipeline(options=options) as p:
    ## CONSO BRANCH ##
    conso_branch = (p
    | 'Read conso' >> beam.io.ReadFromText(INPUT_CONSO)
    | "conso to DICT" >> beam.Map(lambda x: dict(zip(["operateur","annee","filiere","code_categorie_consommation","libelle_categorie_consommation","code_grand_secteur","libelle_grand_secteur","code_naf","libelle_secteur_naf2","conso","pdl","indqual","nombre_mailles_secretisees","code_region","libelle_region"], x.split(";"))))
    | "Group by code_nafsqd" >> beam.Map(lambda d: (d["code_naf"], d))
    )
    
    conso_branch | "Write conso to Bucket" >> beam.io.WriteToText(OUTPUT_CONSO_BUCKET)
    
    ## NAF BRANCH ##
    naf_branch = (p
    | 'Read naf' >> beam.io.ReadFromText(INPUT_NAF)
    | "naf to DICT" >> beam.Map(lambda x: dict(zip(["ligne", "code_naf", "int_v2_40c"], x.split(";"))))
    | "Group by code_nagrzgf" >> beam.Map(lambda d: (d["code_naf"], d))
    )
    
    jointure_pc = (
        {'conso': conso_branch, 'naf': naf_branch}
        | beam.CoGroupByKey()
        | "Création de la jointure" >> beam.ParDo(conso_join_naf)
        | "Filtrer les résultats" >> beam.Filter(lambda x: x is not None)
        | "Write joined conso to BigQuery" >> beam.io.WriteToBigQuery(
        table= TABLE_JOIN,
        schema=schema_join,
        create_disposition=bigquery.job.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.job.WriteDisposition.WRITE_APPEND
        )
    )

    
    p.run().wait_until_finish() 


