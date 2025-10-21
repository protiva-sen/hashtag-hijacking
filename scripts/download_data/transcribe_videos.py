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
    return {}

def save_state(state):
    with open(TRANS_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4) 

def is_done(state, query, start_date, video_id):
    return query in state and start_date in state[query] and video_id in state[query][start_date]

def mark_done(state, query, start_date, video_id):
    if query not in state:
        state[query] = {}
    if start_date not in state[query]:
        state[query][start_date] = {}
    state[query][start_date][video_id] = "done"

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

def fetch_incomplete(state, conn, search_query, start_date, end_date):
    incomplete = []
    
    all_video_ids = get_video_ids(conn, search_query, start_date, end_date)
    
    for video_id in all_video_ids:
        if not is_done(state, search_query, start_date, video_id):
            incomplete.append(video_id)
    
    return incomplete

def get_video_audio(video_id):
    COOKIE_FILE ="cookies.txt"

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
        'cookiefile': COOKIE_FILE,
        'retries': 5,                 
        'sleep_interval': 5,         
        'min_sleep_interval': 10,      
        'max_sleep_interval': 30,    
        'downloader_args': {'http_chunk_size': 1048576},
        'extractor_args': {'youtube': {'player_client': ['mweb']}}, 
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
        print(f"Error downloading audio for video {video_id}: {e}", flush=True)
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
    query = args.query

    model = whisper.load_model(model_name)
    state = load_state()
    conn = sqlite3.connect(args.db_file)
    ensure_transcript_table(conn)

    csv_filename = f"transcripts_{args.query}_{args.start_date}_{args.end_date}.csv"
    csv_path = os.path.join(CSV_OUTPUT_DIR, csv_filename)

    incomplete_video_ids = fetch_incomplete(state, conn, query, args.start_date, args.end_date)

    print(f"\n{len(incomplete_video_ids)} videos to process", flush=True)
    
    if not incomplete_video_ids:
        print("No videos to process. All done!", flush=True)
        conn.close()
        return

    for video_id in tqdm(incomplete_video_ids, desc=f"Processing videos for {query} from {args.start_date} to {args.end_date}"):
        if args.DEBUG:
            print(f"Processing video: {video_id}", flush=True)
        
        try:
            audio_path = get_video_audio(video_id)
            if not audio_path:
                print(f"Failed to download audio for video {video_id}", flush=True)
                continue
            
            text = model.transcribe(audio_path)["text"]
            save_text_to_sql(conn, video_id, text)
            save_transcript_to_csv(video_id, text, query, csv_path)
            
            if os.path.exists(audio_path):
                os.remove(audio_path)
            
            mark_done(state, query, args.start_date, video_id)
            save_state(state)
            
        except Exception as e:
            print(f"[Error] Video {video_id}: {e}", flush=True)
            
            with open('transcription_error_log.txt', 'a') as error_file:
                error_file.write(
                    f"Error for video_id '{video_id}' (query: '{query}', "
                    f"time_range: {args.start_date} to {args.end_date}): {e}\n"
                )
            
            save_state(state)
            continue

    delete_temp_audios()
    conn.close()

if __name__ == "__main__":  
    main()