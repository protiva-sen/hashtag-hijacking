import requests
from config import API_KEY
from utils import to_rfc3339, clean_text

def fetch_videos(query, start, end):
    videos = []
    next_page_token = None

    while True:
        search_url = "https://www.googleapis.com/youtube/v3/search"
        search_params = {
            "q": query,
            "part": "id",
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "publishedAfter": to_rfc3339(start),
            "publishedBefore": to_rfc3339(end),
            "key": API_KEY
        }
        if next_page_token:
            search_params["pageToken"] = next_page_token

        search_response = requests.get(search_url, params=search_params).json()
        video_ids = [item["id"]["videoId"] for item in search_response.get("items", [])]

        if not video_ids:
            break

        # Get video details
        details_url = "https://www.googleapis.com/youtube/v3/videos"
        details_params = {
            "part": "snippet,statistics,contentDetails",
            "id": ",".join(video_ids),
            "key": API_KEY
        }
        details_response = requests.get(details_url, params=details_params).json()

        # Get channel details
        channel_ids = [item["snippet"]["channelId"] for item in details_response.get("items", [])]
        unique_channel_ids = list(set(channel_ids))
        channel_map = {}

        for i in range(0, len(unique_channel_ids), 50):
            chunk = unique_channel_ids[i:i + 50]
            channel_url = "https://www.googleapis.com/youtube/v3/channels"
            channel_params = {
                "part": "snippet,statistics",
                "id": ",".join(chunk),
                "key": API_KEY
            }
            channel_response = requests.get(channel_url, params=channel_params).json()
            for ch in channel_response.get("items", []):
                channel_map[ch["id"]] = ch

        for item in details_response.get("items", []):
            vid = item["id"]
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            channel_id = snippet.get("channelId", "")
            channel = channel_map.get(channel_id, {})
            channel_snippet = channel.get("snippet", {})
            channel_stats = channel.get("statistics", {})

            videos.append((
                vid,
                clean_text(snippet.get("title", "")),
                clean_text(snippet.get("description", "")),
                snippet.get("publishedAt", ""),
                channel_id,
                clean_text(snippet.get("channelTitle", "")),
                str(snippet.get("tags", [])),
                snippet.get("categoryId", ""),
                snippet.get("defaultAudioLanguage", ""),
                snippet.get("defaultLanguage", ""),
                content.get("duration", ""),
                content.get("dimension", ""),
                content.get("definition", ""),
                content.get("caption", ""),
                content.get("licensedContent", False),
                content.get("projection", ""),
                int(stats.get("viewCount", 0)),
                int(stats.get("likeCount", 0)),
                int(stats.get("favoriteCount", 0)),
                int(stats.get("commentCount", 0)),
                channel_snippet.get("customUrl", ""),
                channel_snippet.get("publishedAt", ""),
                int(channel_stats.get("viewCount", 0)),
                int(channel_stats.get("subscriberCount", 0)),
                "Yes" if channel_stats.get("hiddenSubscriberCount", False) else "No",
                int(channel_stats.get("videoCount", 0)),
                channel_snippet.get("country", "")
            ))

        next_page_token = search_response.get("nextPageToken")
        if not next_page_token:
            break

    return videos
