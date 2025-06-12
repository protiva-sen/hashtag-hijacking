from config import API_KEY
import requests

def fetch_comments(video_id):
    comments = []
    next_page_token = None

    while True:
        url = "https://www.googleapis.com/youtube/v3/commentThreads"
        params = {
            "part": "snippet",
            "videoId": video_id,
            "maxResults": 100,
            "textFormat": "plainText",
            "key": API_KEY
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        response = requests.get(url, params=params).json()
        for item in response.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append((
                item["id"],
                video_id,
                snippet.get("textDisplay", ""),
                snippet.get("authorDisplayName", ""),
                snippet.get("publishedAt", "")
            ))

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return comments
