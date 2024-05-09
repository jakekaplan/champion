import requests

def get_post_ids() -> list[int]:
    content = requests.get('https://hacker-news.firebaseio.com/v0/topstories.json?print=pretty').json()

    return 'Hacker News'

def get_comments_from_posts(url: str) -> list[str]:

    return 'Hacker News'

def get_sentiment(content: str) -> float:

    return 0.5


if __name__ == '__main__':
    print(get_post_ids())p