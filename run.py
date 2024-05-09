import modal
import os
import json
import pendulum
from uuid import uuid4
from google.oauth2 import service_account
from google.cloud import storage


BUCKET_NAME = "jake-adam-kevin"

image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements("requirements.txt")

app = modal.App("scrape-data-app", image=image)

@app.function(secrets=[])
def f():
    print(os.environ["OPEN_AI_SECRET"])


def get_gcs_client() -> storage.Client:
    service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = storage.Client(credentials=credentials)
    return client


@app.function(
    schedule=modal.Period(minutes=1),
    secrets=[
        modal.Secret.from_name("jake-adam-kevin-creds"),
        modal.Secret.from_name("open-ai-secret"),
    ])
def upload_data():
    client = get_gcs_client()
    bucket = client.bucket("jake-adam-kevin")
    file_name = f"{pendulum.now().format('YYYY-MM-DD-HH-mm-ss')}-{uuid4()}"
    blob = bucket.blob(file_name)

    data = """{"post_id": 1, "comment_id": 2, "sentiment": 0.5}"""
    blob.upload_from_string(data)


@app.local_entrypoint()
def main():
    upload_data.remote()


if __name__ == '__main__':
    main()
