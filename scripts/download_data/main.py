import argparse
from datetime import datetime
from utils import generate_hourly_ranges
from db_manager import init_db
from video_collector import fetch_videos
from comment_collector import fetch_comments

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', type=str, required=True)
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    args = parser.parse_args()

    query = args.query
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    conn = init_db()
    c = conn.cursor()

    for start, end in generate_hourly_ranges(start_date, end_date):
        print(f"Collecting {query} from {start} to {end}")
        videos = fetch_videos(query, start, end)
        for video in videos:
            c.execute("""
                INSERT OR IGNORE INTO videos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, video)
            comments = fetch_comments(video[0])
            for comment in comments:
                c.execute("""
                    INSERT OR IGNORE INTO comments VALUES (?,?,?,?,?)
                """, comment)
        conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
