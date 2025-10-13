import whisper
from tqdm import tqdm
import sqlite3
import argparse
import os
import json
import yt_dlp
import csv
from datetime import datetime, timedelta


DEFAULT_DB_FILE = "/netfiles/compethicslab/hashtag-hijacking/youtube.db"
TRANS_STATE_FILE = "/netfiles/compethicslab/hashtag-hijacking/transcribe_videos_state.json"
Temp_dir = "temp_audios"
CSV_OUTPUT_DIR = "transcripts_csv"

os.makedirs(Temp_dir, exist_ok=True)
os.makedirs(CSV_OUTPUT_DIR, exist_ok=True)
    

def load_state():
    if os.path.exists(TRANS_STATE_FILE):
        with open(TRANS_STATE_FILE, 'r') as f:
            return json.load(f)
    else:
        return {"done": []}   

def save_state(state):
    with open(TRANS_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2) 

def is_done(state, query, start):
    return query in state and start in state[query]


def mark_done(state, query, start):
    if query not in state:
        state[query] = {}
    state[query][start] = "done"
    save_state(state)
    

def fetch_incomplete(query, start_date, end_date):
    incomplete = []

    state = load_state()

    start_dt = datetime.fromisoformat(start_date)
    end_dt = datetime.fromisoformat(end_date)
    current_dt = start_dt

    while current_dt < end_dt:
        day_str = current_dt.date().isoformat()

        if is_done(state, query, day_str):
            print(f"{query} on {day_str} already done")
        else:
            incomplete.append((query, day_str, (current_dt + timedelta(days=1)).date().isoformat()))

        current_dt += timedelta(days=1)

    return incomplete

def get_video_ids(conn):
    query = 'SELECT DISTINCT video_id FROM youtube_videos'
    return [row[0] for row in conn.execute(query).fetchall()]
    
def get_video_ids(conn, search_query, start_date, end_date):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT DISTINCT video_id 
        FROM youtube_videos
        WHERE search_query = ?
          AND published_at >= ?
          AND published_at < ?
    """, (search_query, start_date, end_date))
    return [row[0] for row in cursor.fetchall()]

def get_video_audio(video_id):
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': os.path.join(Temp_dir, f'{video_id}.%(ext)s'),
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '192',
        }],
        'quiet': True,
        'no_warnings': True,
    }
    
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
            
        audio_path = os.path.join(Temp_dir, f"{video_id}.mp3")
        if os.path.exists(audio_path):
            return audio_path
        else:
            return None
        
    except Exception as e:
        print(f"Error downloading audio for video {video_id}: {e}")
        return None
    
def ensure_transcript_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS video_transcripts (
            video_id TEXT PRIMARY KEY,
            transcript TEXT
        )
    """)
    conn.commit()

def save_text_to_sql(conn, video_id, text):
    query = 'INSERT OR REPLACE INTO video_transcripts (video_id, transcript) VALUES (?, ?)'
    conn.execute(query, (video_id, text))
    conn.commit()

def save_transcript_to_csv(video_id, transcript, query, csv_file):
    file_exists = os.path.exists(csv_file)
    
    with open(csv_file, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        if not file_exists:
            writer.writerow(['video_id', 'query', 'transcript'])
        
        writer.writerow([
            video_id,
            query,
            transcript
        ])

def delete_temp_audios():
    for file in os.listdir(Temp_dir):
        file_path = os.path.join(Temp_dir, file)
        if os.path.isfile(file_path):
            os.remove(file_path)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_name', type=str, required=True, help="Model needed for whisper")
    parser.add_argument('--query', type=str, required=True, help="Hashtag needed to run")
    parser.add_argument('--start_date', type=str, required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument('--end_date', type=str, required=True, help="End date in YYYY-MM-DD")
    parser.add_argument('--db_file', type=str, default=DEFAULT_DB_FILE, help="Path to SQLite database file")    
    parser.add_argument('--DEBUG', action='store_true', help="Run in debug mode with fewer queries")
    args = parser.parse_args()
    model_name = args.model_name

    model = whisper.load_model(model_name)
    state = load_state()
    conn = sqlite3.connect(DEFAULT_DB_FILE)
    ensure_transcript_table(conn)

    csv_filename = f"transcripts_{args.query}_{args.start_date}_{args.end_date}.csv"
    csv_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)

    incomplete_queries = fetch_incomplete(args.query, args.start_date, args.end_date)

    for query, start, end in tqdm(incomplete_queries):
        if is_done(state, query, start):
            continue

        try: 
            video_ids = get_video_ids(conn, query, start, end)
            print(f"\n {len(video_ids)} videos to process")
            
            if not video_ids:
                mark_done(state, query, start)
                continue

            for video_id in tqdm(video_ids, desc=f"Processing videos for {query} from {start} to {end}", disable=args.DEBUG):
                if args.DEBUG:
                    print(f"Processing video: {video_id}")
                audio_path = get_video_audio(video_id)
                if not audio_path:
                    print(f"Failed to download audio for video {video_id}")
                    continue
                text = model.transcribe(audio_path)["text"]
                save_text_to_sql(conn, video_id, text)
                save_transcript_to_csv(video_id, text, query, csv_path)
                os.remove(audio_path)

            mark_done(state, query, start)
            delete_temp_audios()
        except Exception as e:
            print(f"Error processing {query} from {start} to {end}: {e}")
            continue

    conn.close()
if __name__ == "__main__":  
    main()
       

