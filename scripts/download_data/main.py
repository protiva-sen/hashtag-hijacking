import argparse
from datetime import datetime, timedelta
from utils import generate_hourly_ranges, load_state, save_state
from db_manager import init_db
from video_collector import fetch_videos, SEARCH_QUERIES
from comment_collector import fetch_comments

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    state = load_state()
    conn = init_db()
    c = conn.cursor()

    for query in SEARCH_QUERIES:
        for start, end in generate_hourly_ranges(start_date, end_date):
            if state and (query < state["last_query"] or (query == state["last_query"] and start < datetime.fromisoformat(state["last_start"]))):
                continue
            print(f"Collecting {query} from {start} to {end}")
            try:
                videos = fetch_videos(query, start, end)
            except RuntimeError as e:
                print(f"Quota ended: {e}. Saving state and exiting...")
                save_state(query, start)
                conn.commit()
                conn.close()
                return

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
            save_state(query, start)

    conn.close()

if __name__ == "__main__":
    main()