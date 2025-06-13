import argparse
from datetime import datetime
from utils import generate_hourly_ranges
from scripts.download_data.youtubeapi import YouTubeAPI
import json

def get_quries(search_queries_file):
    with open(search_queries_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]
    
def fetch_incomplete():
    # input = Database + Search parameters
    # output = Incomplete queries + hours

    pass

# create a database of queries that need to be run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True)
    parser.add_argument('--end_date', type=str, required=True)
    parser.add_argument('--queries', type=str, required=True, help="text file with search queries, one per line")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    search_queries_file = args.queries
    SEARCH_QUERIES = get_quries(search_queries_file)

    with open('config.json', 'r') as f:
        config = json.load(f)

    # make all data queries in youtube_db
    # get all queries in youtube_db

    youtube_api = YouTubeAPI(config['API_KEY'])
    try:
        videos = youtube_api.fetch_data(
            query, start, end
        )
        videos_df = pd.DataFrame(videos)
        videos_df.to_sql(
            'youtube_videos',
            con=engine,
            if_exists='append',
            index=False
        )
        # push to database
    except Exception as e:
        # save the error query and file
        query = youtube_api.curr_query
        start = youtube_api.curr_start
        end = youtube_api.curr_end
        page_num = youtube_api.curr_page


if __name__ == "__main__":
    main()