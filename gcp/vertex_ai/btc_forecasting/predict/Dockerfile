FROM gcr.io/deeplearning-platform-release/tf2-gpu.2-8

WORKDIR /

ENV AIP_STORAGE_URI=gs://nvallot_bucket/job_outputs/model
ENV AIP_HEALTH_ROUTE=/ping
ENV AIP_PREDICT_ROUTE=/predict
ENV AIP_HTTP_PORT=8080

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENTRYPOINT ["python", "predict.py"]