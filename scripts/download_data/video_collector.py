from config import API_KEY
from utils import to_rfc3339
import requests

def fetch_videos(query, start, end):
    videos = []
    next_page_token = None

    while True:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "q": query,
            "part": "id,snippet",
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "publishedAfter": to_rfc3339(start),
            "publishedBefore": to_rfc3339(end),
            "key": API_KEY,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        res = requests.get(url, params=params).json()

        for item in res.get("items", []):
            video_id = item["id"]["videoId"]
            snippet = item["snippet"]
            videos.append((video_id, snippet["publishedAt"], snippet["title"], snippet["description"]))

        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break

    return videos
