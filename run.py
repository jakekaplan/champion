import json
import os
from typing import Any, List, Optional
from uuid import uuid4

import modal
import pendulum
import requests
from google.cloud import storage
from google.oauth2 import service_account
from marvin import fn
from pydantic import BaseModel, Field, HttpUrl

BUCKET_NAME = "jake-adam-kevin"
image = modal.Image.debian_slim(python_version="3.11").pip_install_from_requirements("requirements.txt")
app = modal.App("scrape-data-app", image=image)


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


def get_comment_sentiments(post_id: int) -> List[CommentSentiment]:
    result: List[CommentSentiment] = []
    for kid in get_detail(post_id).kids:
        sentiment_score = fn(sentiment)(get_detail(kid).text)
        result.append(CommentSentiment(comment_id=kid, post_id=post_id, sentiment=sentiment_score))
    return result


def get_top_hackernews_comments_sentiments() -> List[CommentSentiment]:
    post_ids = get_post_ids()
    result: List[CommentSentiment] = []
    for post_id in post_ids:
        result.extend(get_comment_sentiments(post_id))
    return result


def get_gcs_client() -> storage.Client:
    service_account_info = json.loads(os.environ["SERVICE_ACCOUNT_JSON"])
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    client = storage.Client(credentials=credentials)
    return client


@app.function(
    schedule=modal.Period(hours=1),
    secrets=[
        modal.Secret.from_name("jake-adam-kevin-creds"),
        modal.Secret.from_name("open-api-secret"),
    ])
def upload_data():
    client = get_gcs_client()
    bucket = client.bucket("jake-adam-kevin")

    print("Getting top hackernews comments sentiments")
    post_ids = get_post_ids()
    for post_id in post_ids:
        comments = get_comment_sentiments(post_id)
        for comment in comments:
            data = comment.model_dump_json()
            file_name = f"{pendulum.now().format('YYYY-MM-DD-HH-mm-ss')}-{comment.comment_id}"
            blob = bucket.blob(file_name)
            blob.upload_from_string(data)
            print(f"Uploading {file_name} to GCS")

    print("All done!")


@app.local_entrypoint()
def main():
    upload_data.remote()


if __name__ == '__main__':
    modal.runner.deploy_app(app)
