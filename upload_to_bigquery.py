from google.cloud import bigquery
import modal
import json
import os
from google.oauth2 import service_account


BUCKET_NAME = "jake-adam-kevin"
image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements("requirements.txt")
app = modal.App("upload-to-big-query-app", image=image)


def get_bigquery_client() -> bigquery.Client:
    service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = bigquery.Client(credentials=credentials)
    return client


@app.function(retries=3, secrets=[
    modal.Secret.from_name("jake-adam-kevin-creds"),
    modal.Secret.from_name("open-api-secret"),
])
@modal.web_endpoint()
def load_to_bigquery(bucket_name: str, file_name: str):
     client = get_bigquery_client()

     table_id = "prefect-sbx-eng-offsite-5-24.jake_adam_kevin.hackernews"

     job_config = bigquery.LoadJobConfig(
          schema=[
               bigquery.SchemaField(name="post_id", field_type="INTEGER"),
               bigquery.SchemaField(name="comment_id", field_type="INTEGER"),
               bigquery.SchemaField(name="sentiment", field_type="FLOAT"),
          ],
          write_disposition="WRITE_APPEND",
          source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
     )
     uri = f"gs://{bucket_name}/{file_name}"

     load_job = client.load_table_from_uri(
     uri, table_id, job_config=job_config
     )  # Make an API request.

     load_job.result()  # Waits for the job to complete.

     destination_table = client.get_table(table_id)  # Make an API request.
     print("Loaded {} rows.".format(destination_table.num_rows))


if __name__ == '__main__':
    modal.runner.deploy_app(app)
