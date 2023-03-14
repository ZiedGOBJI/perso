
PROJECT_ID = 'glossy-precinct-371813'
BUCKET = 'bucket_zied'

INPUT_NAF = 'gs://{}/dataflow/naf_short.csv'.format(BUCKET)
INPUT_CONSO = 'gs://{}/dataflow/conso_short.csv'.format(BUCKET)

OUTPUT_CONSO_BUCKET = 'gs://{}/dataflow/output/'.format(BUCKET)

TABLE_CONSO ='glossy-precinct-371813.zgi_dataflow_db.TABLE_CONSO'
TABLE_JOIN = 'glossy-precinct-371813.zgi_dataflow_db.TABLE_JOIN'

STAGING_LOC = 'gs://{}/dataflow/staging/'.format(BUCKET)
TMP_LOC = 'gs://{}/dataflow/temp/'.format(BUCKET)

REGION = 'us-central1'
JOB_NAME = 'load-data-to-bigquery-final'
RUNNER = 'DataflowRunner'