from config import API_KEY
from utils import to_rfc3339, clean_text
import requests

SEARCH_QUERIES = [
    "#bmw", "#mrbeast", "#whatsappstatus", "#whatsapp", "#islamic",
    "#quran", "#allah", "#palestine", "#fortnite", "#bitcoin",
    "#travelvlog", "#travelling"
]

def fetch_videos(query, start, end):
    videos = []
    next_page_token = None
    quota_used = 0

    while True:
        url = "https://www.googleapis.com/youtube/v3/search"
        params = {
            "q": query,
            "part": "id",
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "publishedAfter": to_rfc3339(start),
            "publishedBefore": to_rfc3339(end),
            "key": API_KEY,
        }
        if next_page_token:
            params["pageToken"] = next_page_token

        res = requests.get(url, params=params)
        quota_used += 100

        if quota_used > 10800:
            raise RuntimeError("API quota exceeded")

        data = res.json()

        for item in data.get("items", []):
            video_id = item["id"]["videoId"]
            detail_url = "https://www.googleapis.com/youtube/v3/videos"
            detail_params = {
                "part": "snippet,statistics,contentDetails",
                "id": video_id,
                "key": API_KEY
            }
            detail = requests.get(detail_url, params=detail_params).json()
            if not detail.get("items"):
                continue
            details = detail["items"][0]
            snippet = details.get("snippet", {})
            statistics = details.get("statistics", {})
            content_details = details.get("contentDetails", {})

            channel_id = snippet.get("channelId", "")
            channel_url = "https://www.googleapis.com/youtube/v3/channels"
            channel_params = {
                "part": "snippet,statistics",
                "id": channel_id,
                "key": API_KEY
            }
            channel_info = requests.get(channel_url, params=channel_params).json()
            channel_snippet = channel_info.get("items", [{}])[0].get("snippet", {})
            channel_stats = channel_info.get("items", [{}])[0].get("statistics", {})

            videos.append((
                video_id,
                clean_text(snippet.get("title", "")),
                clean_text(snippet.get("description", "")),
                snippet.get("publishedAt", ""),
                snippet.get("channelId", ""),
                clean_text(snippet.get("channelTitle", "")),
                str(snippet.get("tags", "")),
                snippet.get("categoryId", ""),
                snippet.get("defaultAudioLanguage", ""),
                content_details.get("duration", ""),
                content_details.get("dimension", ""),
                content_details.get("definition", ""),
                content_details.get("caption", ""),
                content_details.get("licensedContent", False),
                content_details.get("projection", ""),
                statistics.get("viewCount", 0),
                statistics.get("likeCount", 0),
                statistics.get("favoriteCount", 0),
                statistics.get("commentCount", 0),
                channel_snippet.get("customUrl", ""),
                channel_snippet.get("publishedAt", ""),
                channel_stats.get("viewCount", 0),
                channel_stats.get("subscriberCount", 0),
                "Yes" if channel_stats.get("hiddenSubscriberCount", False) else "No",
                channel_stats.get("videoCount", 0),
                channel_snippet.get("country", "")
            ))

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return videos