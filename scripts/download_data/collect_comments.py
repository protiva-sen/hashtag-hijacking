import json
import os
import sqlite3
import argparse
import pandas as pd
from tqdm import tqdm
from commentapi import YouTubeCommentAPI

COMMENT_STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/state_comments.json"
DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"

def load_state():
    if os.path.exists(COMMENT_STATE_FILE):
        with open(COMMENT_STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(COMMENT_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def get_video_ids(conn):
    query = "SELECT DISTINCT video_id FROM youtube_videos"
    return [row[0] for row in conn.execute(query).fetchall()]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--DEBUG', action='store_true', help="Debug mode: limit number of videos")
    parser.add_argument('--max_comments', type=int, default=1000, help="Max number of comments to collect per video")
    args = parser.parse_args()

    with open('config.json', 'r') as f:
        config = json.load(f)
    api_key = config.get("API_KEY")

    conn = sqlite3.connect(DEFAULT_DB_FILE)
    video_ids = get_video_ids(conn)
    state = load_state()
    api = YouTubeCommentAPI(api_key)

    if args.DEBUG:
        video_ids = video_ids[:10]

    for video_id in tqdm(video_ids, desc="Fetching comments"):
        if state.get(video_id) == "done":
            continue

        try:
            comments = api.fetch_comments(video_id, max_comments=args.max_comments)
            if not comments:
                state[video_id] = "done"
                continue

            comments_df = pd.DataFrame(comments, columns=[
                "video_id",
                "comment_id",
                "author_display_name",
                "author_channel_id",
                "published_at",
                "text_original",
                "like_count"
            ])

            comments_df.to_sql('youtube_comments', con=conn, if_exists='append', index=False)
            state[video_id] = "done"
            save_state(state)

        except Exception as e:
            print(f"[ERROR] {video_id}: {e}")
            with open("error_comments.txt", "a") as logf:
                logf.write(f"{video_id}: {e}\n")
            save_state(state)
            continue

    conn.close()

if __name__ == "__main__":
    main()
