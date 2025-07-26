import requests

class YouTubeCommentAPI:
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self.comment_url = "https://www.googleapis.com/youtube/v3/commentThreads"

    def fetch_comments(self, video_id, max_comments=1000):
        comments = []
        page_token = None

        while True:
            if len(comments) >= max_comments:
                break

            params = {
                "part": "snippet",
                "videoId": video_id,
                "maxResults": 100,
                "textFormat": "plainText",
                "key": self.API_KEY
            }
            if page_token:
                params["pageToken"] = page_token

            res = requests.get(self.comment_url, params=params)
            if res.status_code == 403:
                raise Exception("Quota error or access denied.")
            res.raise_for_status()
            data = res.json()

            for item in data.get("items", []):
                snippet = item["snippet"]["topLevelComment"]["snippet"]
                comments.append({
                    "video_id": video_id,
                    "comment_id": item["id"],
                    "author_display_name": snippet.get("authorDisplayName", ""),
                    "author_channel_id": snippet.get("authorChannelId", {}).get("value", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "text_original": snippet.get("textOriginal", ""),
                    "like_count": snippet.get("likeCount", 0)
                })

                if len(comments) >= max_comments:
                    break

            if len(comments) >= max_comments or "nextPageToken" not in data:
                break
            page_token = data["nextPageToken"]

        return comments
