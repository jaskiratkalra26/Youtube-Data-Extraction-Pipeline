
from googleapiclient.discovery import build
import datetime

API_KEY = "AIzaSyBV5VN40HOWGO9sPUSj3rsESMzRvxs79EQ"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def test_new_query():
    youtube = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)
    
    q_val = "vlog|review|tutorial -news -live"
    print(f"Testing q='{q_val}'")
    
    try:
        search_response = youtube.search().list(
            q=q_val,
            part="id,snippet",
            order="date",
            type="video",
            maxResults=10
        ).execute()

        items = search_response.get("items", [])
        print(f"-> Found {len(items)} items.")
        for item in items:
            title = item['snippet']['title']
            channel = item['snippet']['channelTitle']
            print(f"   - {title} ({channel})")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_new_query()
