
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import datetime

# --- Configuration ---
API_KEY = "AIzaSyBV5VN40HOWGO9sPUSj3rsESMzRvxs79EQ" 
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def search_videos(query_string=None):
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

    try:
        # Calculate a recent time to ensure we are getting fresh content if needed
        # But for 'order=date', it should return the newest first anyway.
        
        print(f"Testing query: '{query_string}'")
        
        search_response = youtube.search().list(
            q=query_string,
            part="id,snippet",
            order="date",
            type="video",
            maxResults=5
        ).execute()

        items = search_response.get("items", [])
        print(f"  -> Found {len(items)} items.")
        for item in items:
            print(f"     - {item['snippet']['title']} ({item['snippet']['publishedAt']})")
            
    except HttpError as e:
        print(f"  -> An HTTP error {e.resp.status} occurred: {e.content}")

if __name__ == "__main__":
    queries_to_test = [" ", "video", "new", "vlog", "|"]
    
    for q in queries_to_test:
        search_videos(q)
        print("-" * 20)
