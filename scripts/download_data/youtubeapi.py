from utils import to_rfc3339, clean_text
import requests

class YouTubeAPI:
    def __init__(self, API_KEY):
        self.API_KEY = API_KEY
        self.search_url = "https://www.googleapis.com/youtube/v3/search"
        self.videos_url = "https://www.googleapis.com/youtube/v3/videos"
        self.channel_url = "https://www.googleapis.com/youtube/v3/channels"
        self.curr_query = None
        self.curr_start = None
        self.curr_end = None
        self.curr_page = None

    def _fetch_ids(self, query, start, end, page_num=None):
        params = {
            "q": query,
            "part": "id",
            "type": "video",
            "order": "date",
            "maxResults": 50,
            "publishedAfter": to_rfc3339(start),
            "publishedBefore": to_rfc3339(end),
            "key": self.API_KEY,
        }
        if page_num:
            params["pageToken"] = page_num
        
        res = requests.get(self.search_url, params=params)
        res.raise_for_status()

        return res.json()
    
    def _fetch_video(self, video_id):
        params = {
            "part": "snippet,statistics,contentDetails",
            "id": video_id,
            "key": self.API_KEY
        }
        res = requests.get(self.videos_url, params=params)
        res.raise_for_status()

        return res.json()
    
    def _fetch_channel(self, channel_id):
        params = {
            "part": "snippet,statistics",
            "id": channel_id,
            "key": self.API_KEY
        }
        res = requests.get(self.channel_url, params=params)
        res.raise_for_status()

        return res.json()


    def fetch_data(self, query, start, end, page_num=None):
        videos = []
        self.curr_query = page_num
        self.query = query
        self.start = start
        self.end = end

        while True:
            ids = self._fetch_ids(query, start, end, self.curr_page)

            for item in ids.get("items", []):
                video_id = item["id"]["videoId"]
                video = self._fetch_video(video_id)
                if not video.get("items"):
                    continue
                video_data = video["items"][0]
                snippet = video_data.get("snippet", {})
                statistics = video_data.get("statistics", {})
                content_details = video_data.get("contentDetails", {})

                channel_id = snippet.get("channelId", "")
        
                channel_info = self._fetch_channel(channel_id)
                channel_snippet = channel_info.get("items", [{}])[0].get("snippet", {})
                channel_stats = channel_info.get("items", [{}])[0].get("statistics", {})

                videos.append({
                    'video_id': video_id,
                    'title': clean_text(snippet.get("title", "")),
                    'description': clean_text(snippet.get("description", "")),
                    'published_at': snippet.get("publishedAt", ""),
                    'channel_id': channel_id,
                    'channel_title': clean_text(snippet.get("channelTitle", "")),
                    'tags': str(snippet.get("tags", "")),
                    'category_id': snippet.get("categoryId", ""),
                    'default_audio_language': snippet.get("defaultAudioLanguage", ""),
                    'duration': content_details.get("duration", ""),
                    'dimension': content_details.get("dimension", ""),
                    'definition': content_details.get("definition", ""),
                    'caption': content_details.get("caption", ""),
                    'licensed_content': content_details.get("licensedContent", False),
                    'projection': content_details.get("projection", ""),
                    'view_count': statistics.get("viewCount", 0),
                    'like_count': statistics.get("likeCount", 0),
                    'favorite_count': statistics.get("favoriteCount", 0),
                    'comment_count': statistics.get("commentCount", 0),
                    'channel_custom_url': channel_snippet.get("customUrl", ""),
                    'channel_published_at': channel_snippet.get("publishedAt", ""),
                    'channel_view_count': channel_stats.get("viewCount", 0),
                    'channel_subscriber_count': channel_stats.get("subscriberCount", 0),
                    'channel_hidden_subscriber_count': "Yes" if channel_stats.get("hiddenSubscriberCount", False) else "No",
                    'channel_video_count': channel_stats.get("videoCount", 0),
                    'channel_country': channel_snippet.get("country", "")
                })

            self.curr_page = ids.get("nextPageToken", None)
            if not self.curr_page:
                break

        return videos
    


# from config import API_KEY
# import requests

# def fetch_comments(video_id):
#     comments = []
#     next_page_token = None

#     while True:
#         url = "https://www.googleapis.com/youtube/v3/commentThreads"
#         params = {
#             "part": "snippet",
#             "videoId": video_id,
#             "maxResults": 100,
#             "textFormat": "plainText",
#             "key": API_KEY
#         }
#         if next_page_token:
#             params["pageToken"] = next_page_token

#         response = requests.get(url, params=params)

#         if response.status_code == 403 and 'commentsDisabled' in response.text:
#             print(f"Comments are disabled for video: {video_id}")
#             comments.append((f"disabled-{video_id}", video_id, "disabled", "", ""))
#             break

#         data = response.json()
#         for item in data.get("items", []):
#             snippet = item["snippet"]["topLevelComment"]["snippet"]
#             comments.append((
#                 item["id"],
#                 video_id,
#                 snippet.get("textDisplay", ""),
#                 snippet.get("authorDisplayName", ""),
#                 snippet.get("publishedAt", "")
#             ))

#         next_page_token = data.get("nextPageToken")
#         if not next_page_token:
#             break

#     return comments
