import requests
from typing import List, Optional, Any
from pydantic import BaseModel, HttpUrl, Field
import openai 
from marvin import fn

client = openai.Client(api_key='BULLSHIT')

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
    for kid in get_detail(post_id).kids[:1]:
        sentiment_score = fn(sentiment)(get_detail(kid).text)
        result.append(CommentSentiment(comment_id=kid, post_id=post_id, sentiment=sentiment_score))
    return result

def get_top_hackernews_comments_sentiments() -> List[CommentSentiment]:
    post_ids = get_post_ids()
    result: List[CommentSentiment] = []
    for post_id in post_ids[:1]:
        result.extend(get_comment_sentiments(post_id))
    return result

if __name__ == '__main__':
    print(get_top_hackernews_comments_sentiments())