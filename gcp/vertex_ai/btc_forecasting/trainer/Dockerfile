FROM gcr.io/deeplearning-platform-release/tf2-gpu.2-3

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
ENTRYPOINT ["python", "trainer.py"]