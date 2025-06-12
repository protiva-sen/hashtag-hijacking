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

        response = requests.get(url, params=params)
        if response.status_code == 403 and 'commentsDisabled' in response.text:
            print(f"Comments are disabled for video: {video_id}")
            comments.append((f"{video_id}_disabled", video_id, "disabled", "", ""))
            break

        data = response.json()
        for item in data.get("items", []):
            snippet = item["snippet"]["topLevelComment"]["snippet"]
            comments.append((
                item["id"],
                video_id,
                snippet.get("textDisplay", ""),
                snippet.get("authorDisplayName", ""),
                snippet.get("publishedAt", "")
            ))

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return comments
