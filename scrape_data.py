import json
import os
from typing import Any, List, Optional

import modal
import pendulum
import requests
from google.cloud import storage
from google.oauth2 import service_account
from marvin import fn
from pydantic import BaseModel, Field, HttpUrl

_gcs_client = None
BUCKET_NAME = "jake-adam-kevin"
image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements("requirements.txt")
app = modal.App("scrape-data-app", image=image)


def get_gcs_client() -> storage.Client:
    global _gcs_client
    if _gcs_client:
        return _gcs_client
    else:
        service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        client = storage.Client(credentials=credentials)
        _gcs_client = client
    return client


class Item(BaseModel):
    by: Optional[str] = None
    descendants: Optional[int] = None
    id: Optional[int] = None
    kids: List[int] = Field(default_factory = list)
    score: Optional[int] = None
    text: Optional[Any] = None
    time: Optional[int] = None
    title: Optional[str] = None
    type: Optional[str] = None
    url: Optional[HttpUrl] = None


class CommentSentiment(BaseModel):
    comment_id: int
    post_id: int
    sentiment: float


def get_post_ids() -> list[int]:
    return requests.get('https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty').json()


def get_detail(id: int) -> Item:
    '''
    All HackerNews posts are represented hierarchically using a common structure.
    '''
    content = requests.get(f'https://hacker-news.firebaseio.com/v0/item/{id}.json?print=pretty').json()
    return Item(**content)


def sentiment(text: str) -> float:
    """
    Returns a sentiment score for `text`
    between -1 (negative) and 1 (positive).
    """


@app.function(retries=3, concurrency_limit=400, secrets=[
        modal.Secret.from_name("jake-adam-kevin-creds"),
        modal.Secret.from_name("open-api-secret"),
])
def get_and_upload_comment_sentiment(info: dict):
    comment_id = info["comment_id"]
    post_id = info["post_id"]

    sentiment_score = fn(sentiment)(get_detail(comment_id).text)
    comment = CommentSentiment(comment_id=comment_id, post_id=post_id, sentiment=sentiment_score)
    gcs_client = get_gcs_client()
    data = comment.model_dump_json()
    file_name = f"{pendulum.now().format('YYYY-MM-DD-HH-mm-ss')}-{comment.comment_id}"
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data)
    return file_name


@app.function()
def upload_data():
    print("Getting hacker news post ids...")
    post_ids = get_post_ids()
    print(f"Found {len(post_ids)} post ids")
    for post_id in post_ids:
        res = get_and_upload_comment_sentiment.map(
            [{"comment_id": comment_id, "post_id": post_id} for comment_id in get_detail(post_id).kids]
        )
        for r in res:
            print("Uploaded:", r)

    print("All done!")


@app.local_entrypoint()
def main():
    upload_data.remote()


if __name__ == '__main__':
    modal.runner.deploy_app(app)
