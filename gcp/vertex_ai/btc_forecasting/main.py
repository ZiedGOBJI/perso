import subprocess 

# INSTALLER LA VERSION 2.3 SINON ERREUR AVEC LIBRAIRIE PROTOBUF..... UTILISER --user SINON PROBLEMES DE DROITS OU PENSER A PASSER SUR UN VENV
subprocess.check_call(['pip', 'install', 'tensorflow==2.3', '--user'])
subprocess.check_call(['pip', 'install', 'protobuf', '--user'])


from google.cloud import aiplatform

REGION = 'europe-west1'
PROJECT_ID = 'glossy-precinct-371813'
model_path = 'gs://bucket_zied_eu/output'
job_name = 'zgi-btc-job'

trainer_container = 'europe-west1-docker.pkg.dev/glossy-precinct-371813/btc-app/btc-model:latest'
predict_container = 'europe-west1-docker.pkg.dev/glossy-precinct-371813/btc-app/btc-predict:latest'

# Initialize the Vertex SDK for Python
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=model_path)

# Create a custom training job
job = aiplatform.CustomContainerTrainingJob(
    display_name=job_name,
    container_uri=trainer_container,
    model_serving_container_image_uri=predict_container
)


# Run the training job
model = job.run(model_display_name=job_name)


# Deploy the model
endpoint = model.deploy(
    deployed_model_display_name=job_name,
    sync=True,
    traffic_percentage=100,
    min_replica_count=1,
    max_replica_count=1,
    accelerator_count=0,
    machine_type='n1-standard-4'
)

response = endpoint.predict([4,5,6])

print('API response: ', response)

print('Predicted MPG: ', response.predictions[0][0])