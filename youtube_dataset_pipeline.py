import os
import time
import datetime
import pandas as pd
import schedule
import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import numpy as np

# --- Configuration ---
API_KEY = os.environ.get("YOUTUBE_API_KEY")  # Use env var or fallback
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

# File names
PENDING_VIDEOS_FILE = "pending_videos.csv"
STATIC_DATA_FILE = "static_data.csv"
HOUR1_DATA_FILE = "hour1_data.csv"
FINAL_DATA_FILE = "final_data.csv"
DATASET_FILE = "dataset.csv"

# --- Helper Functions ---

def get_youtube_service():
    """Initializes and returns the YouTube API service."""
    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, developerKey=API_KEY)

def load_csv(filename):
    """Loads a CSV file into a DataFrame, or returns an empty DataFrame if file doesn't exist."""
    if os.path.exists(filename):
        return pd.read_csv(filename)
    return pd.DataFrame()

def save_csv(df, filename, mode='w', header=True):
    """Saves a DataFrame to a CSV file."""
    if mode == 'a' and os.path.exists(filename):
         # If appending and file exists, don't write header again
         df.to_csv(filename, mode=mode, header=False, index=False)
    else:
        df.to_csv(filename, mode=mode, header=header, index=False)

def get_category_map(youtube):
    """Fetches all video categories and returns a dictionary mapping ID to Title."""
    try:
        response = youtube.videoCategories().list(
            part="snippet",
            regionCode="US"
        ).execute()
        
        category_map = {}
        for item in response.get("items", []):
            category_id = item["id"]
            title = item["snippet"]["title"]
            category_map[category_id] = title
            
        return category_map
    except Exception as e:
        print(f"Error fetching category map: {e}")
        return {}

def iso_duration_to_seconds(iso_duration):
    """Converts ISO 8601 duration string to seconds."""
    try:
        dur = isodate.parse_duration(iso_duration)
        return int(dur.total_seconds())
    except Exception as e:
        print(f"Error parsing duration {iso_duration}: {e}")
        return 0

# --- Core Pipeline Functions ---

def discover_videos():
    """
    Step 1:
    - Search for newly uploaded videos.
    - Saves initial metadata to pending_videos.csv.
    """
    print(f"[{datetime.datetime.now()}] Starting discover_videos...")
    youtube = get_youtube_service()
    
    # --- Configuration for discover_videos ---
    MAX_PAGES = 4 # Fetch up to 4 pages (approx 200 videos)

    try:
        new_videos = []
        next_page_token = None
        pages_fetched = 0

        # Calculate strict time window (last 2 hours)
        # This prevents picking up old videos if the API drifts back in time
        time_window_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=2)
        published_after_str = time_window_start.isoformat().replace("+00:00", "Z")

        while pages_fetched < MAX_PAGES:
            print(f"Fetching page {pages_fetched + 1}...")
            
            # Search for new videos with a broad query to capture diverse content
            # Using "vlog|review|tutorial -news -live" to target creator content and avoid news broadcasts
            search_response = youtube.search().list(
                q="vlog|review|tutorial -news -live",
                part="id,snippet",
                order="date",
                type="video",
                publishedAfter=published_after_str, # FORCE API to only look at recent videos
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            items = search_response.get("items", [])
            for item in items:
                video_id = item["id"]["videoId"]
                channel_id = item["snippet"]["channelId"]
                published_at = item["snippet"]["publishedAt"]  # e.g. 2026-02-25T14:00:00Z
                
                # Double check time (parsing can be tricky, but API filter usually works)
                try:
                    pub_dt = isodate.parse_datetime(published_at)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=datetime.timezone.utc)
                    
                    if pub_dt < time_window_start:
                        print(f"Skipping video {video_id}: Too old ({published_at})")
                        continue
                except:
                    pass

                new_videos.append({
                    "video_id": video_id,
                    "channel_id": channel_id,
                    "publish_time": published_at,
                    "status": "new" # Status tracking: new -> processing -> processed
                })
            
            next_page_token = search_response.get("nextPageToken")
            pages_fetched += 1
            
            if not next_page_token:
                break

        if not new_videos:
            print("No new videos found via API.")
            return

        # Create DataFrame from collected videos
        # Note: There might be duplicates across pages if API results shift rapidly, so we drop them
        new_df = pd.DataFrame(new_videos)
        new_df.drop_duplicates(subset=['video_id'], inplace=True)

        # Load existing pending videos to avoid duplicates
        existing_df = load_csv(PENDING_VIDEOS_FILE)
        
        if not existing_df.empty:
            # Filter out videos that are already in pending_videos.csv
            existing_ids = set(existing_df['video_id'].unique())
            new_df = new_df[~new_df['video_id'].isin(existing_ids)]

        if new_df.empty:
            print("No new unique videos found.")
            return

        # Append new unique videos
        save_csv(new_df, PENDING_VIDEOS_FILE, mode='a', header=not os.path.exists(PENDING_VIDEOS_FILE))
        print(f"Added {len(new_df)} new videos to {PENDING_VIDEOS_FILE}")

        # Trigger processing for these new videos immediately or wait for next cycle?
        # Let's process the 'new' videos immediately for static stats if they pass filters
        process_new_videos()

    except HttpError as e:
        print(f"An HTTP error {e.resp.status} occurred: {e.content}")
    except Exception as e:
        print(f"An error occurred in discover_videos: {e}")

def get_channel_stats(youtube, channel_id):
    """Fetches channel statistics, specifically subscriber count and upload playlist ID."""
    try:
        response = youtube.channels().list(
            part="statistics,contentDetails",
            id=channel_id
        ).execute()

        items = response.get("items", [])
        if not items:
            return None, None

        stats = items[0]["statistics"]
        content_details = items[0]["contentDetails"]
        
        subscriber_count = int(stats.get("subscriberCount", 0))
        uploads_playlist_id = content_details["relatedPlaylists"]["uploads"]

        return subscriber_count, uploads_playlist_id
    except Exception as e:
        print(f"Error fetching channel stats for {channel_id}: {e}")
        return None, None

def get_avg_last10_views(youtube, uploads_playlist_id):
    """Computes the average views of the last 10 videos in the uploads playlist."""
    try:
        # Get last 10 items from playlist
        playlist_response = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=uploads_playlist_id,
            maxResults=10
        ).execute()

        video_ids = [item["contentDetails"]["videoId"] for item in playlist_response.get("items", [])]
        
        if not video_ids:
            return 0

        # Get stats for these videos
        # Note: video_ids needs to be comma separated string
        ids_str = ",".join(video_ids)
        videos_response = youtube.videos().list(
            part="statistics",
            id=ids_str
        ).execute()

        view_counts = []
        for item in videos_response.get("items", []):
            views = int(item["statistics"].get("viewCount", 0))
            view_counts.append(views)

        if not view_counts:
            return 0
            
        return np.mean(view_counts)

    except Exception as e:
        print(f"Error computing avg_last10_views: {e}")
        return 0

def get_video_details(youtube, video_id):
    """Fetches video duration (seconds) and category ID."""
    try:
        response = youtube.videos().list(
            part="contentDetails,snippet",
            id=video_id
        ).execute()
        
        items = response.get("items", [])
        if not items:
            return 0, None
            
        iso_duration = items[0]["contentDetails"]["duration"]
        category_id = items[0]["snippet"]["categoryId"]
        
        duration = iso_duration_to_seconds(iso_duration)
        return duration, category_id
    except Exception as e:
        print(f"Error get_video_details for {video_id}: {e}")
        return 0, None

def process_new_videos():
    """
    Steps 2, 3, 4:
    - Reads from pending_videos.csv where status is 'new'.
    - Filters by subscriber count.
    - Collects static features.
    - Updates status in pending_videos.csv.
    """
    print(f"[{datetime.datetime.now()}] Processing new videos for static data...")
    youtube = get_youtube_service()
    
    pending_df = load_csv(PENDING_VIDEOS_FILE)
    if pending_df.empty:
        print("No pending videos file found.")
        return

    # Check if 'status' column exists, if not create it (migration handling)
    if 'status' not in pending_df.columns:
        pending_df['status'] = 'new'

    # Filter for 'new' videos
    mask_new = pending_df['status'] == 'new'
    videos_to_process = pending_df[mask_new]

    if videos_to_process.empty:
        print("No new videos to process.")
        return

    static_data_list = []
    
    # Fetch category map once
    category_map = get_category_map(youtube)

    # Iterate and process
    for index, row in videos_to_process.iterrows():
        video_id = row['video_id']
        channel_id = row['channel_id']
        publish_time = row['publish_time']

        print(f"Checking features for video: {video_id}")

        # 2. Filter Channels by Subscriber Count
        sub_count, uploads_playlist_id = get_channel_stats(youtube, channel_id)
        
        # Valid range check
        if sub_count is None or not (10000 <= sub_count <= 100000):
            print(f"  -> Channel {channel_id} rejected (Subs: {sub_count})")
            pending_df.at[index, 'status'] = 'rejected'
            continue
            
        # 3. Collect Static Features: Duration and Category
        duration_sec, cat_id = get_video_details(youtube, video_id)
        category_name = category_map.get(cat_id, "Unknown")
        
        # 4. Compute avg_last10_views
        avg_views = get_avg_last10_views(youtube, uploads_playlist_id)

        # Record static data
        static_data_list.append({
            "video_id": video_id,
            "channel_id": channel_id,
            "publish_time": publish_time,
            "category": category_name,
            "subscriber_count": sub_count,
            "avg_last10_views": avg_views,
            "duration_seconds": duration_sec
        })
        
        print(f"  -> Video {video_id} accepted.")
        pending_df.at[index, 'status'] = 'monitoring' # Ready for Hour 1 monitoring

    # Save static data
    if static_data_list:
        static_df = pd.DataFrame(static_data_list)
        
        # Robust Schema Migration
        if os.path.exists(STATIC_DATA_FILE):
            try:
                # Read just the header first to check columns
                existing_header = pd.read_csv(STATIC_DATA_FILE, nrows=0).columns.tolist()
                new_columns = static_df.columns.tolist()
                
                # Find columns present in new data but missing in file
                missing_cols = [col for col in new_columns if col not in existing_header]
                
                if missing_cols:
                    print(f"Migrating {STATIC_DATA_FILE} schema: Adding {missing_cols}")
                    # Read full file
                    existing_df = pd.read_csv(STATIC_DATA_FILE)
                    
                    # Backfill missing columns in existing data
                    for col in missing_cols:
                        if col == 'category':
                            existing_df[col] = 'Unknown'
                        elif col == 'duration_seconds':
                            existing_df[col] = 0.0 # Or some default
                        else:
                            existing_df[col] = None 
                            
                    # Concatenate and Overwrite
                    # We use sort=False to keep order, but we should align to new schema
                    combined_df = pd.concat([existing_df, static_df], ignore_index=True)
                    
                    # Ensure we keep the order of columns from the NEW schema (which is the desired target)
                    combined_df = combined_df[new_columns]
                    
                    save_csv(combined_df, STATIC_DATA_FILE, mode='w', header=True)
                else:
                    # Columns match (or file has MORE columns, which is fine for append usually, 
                    # but strictly we should ensure column ORDER matches before appending without header)
                    
                    # Align static_df columns to existing file order to be safe
                    # (in case dict order varied, though unlikely in Py3.7+)
                    common_cols = [c for c in existing_header if c in static_df.columns]
                    # If existing file has columns we don't have, we might have an issue, 
                    # but we assume we are just adding columns.
                    
                    # Reorder static_df to match existing_header for safe append
                    static_df_to_append = static_df[existing_header]
                    save_csv(static_df_to_append, STATIC_DATA_FILE, mode='a', header=False)
                    
            except Exception as e:
                print(f"Error checking schema for {STATIC_DATA_FILE}: {e}")
                # Fallback: Just append and hope
                save_csv(static_df, STATIC_DATA_FILE, mode='a', header=False)
        else:
            # File doesn't exist, Create new
            save_csv(static_df, STATIC_DATA_FILE, mode='w', header=True)

        print(f"Saved {len(static_df)} records to {STATIC_DATA_FILE}")

    # Update pending_videos.csv with new statuses
    save_csv(pending_df, PENDING_VIDEOS_FILE, mode='w')
    print("Updated pending_videos.csv statuses.")

def collect_hour1_stats():
    """
    Step 5:
    - Checks pending_videos.csv for videos in 'monitoring' status.
    - If (current_time - publish_time) >= 1 hour, fetch stats.
    - Saves to hour1_data.csv.
    - Updates status to 'monitoring_final'.
    """
    print(f"[{datetime.datetime.now()}] Collecting Hour 1 Stats...")
    youtube = get_youtube_service()
    
    pending_df = load_csv(PENDING_VIDEOS_FILE)
    if pending_df.empty or 'status' not in pending_df.columns:
        return

    # Check for videos ready for hour 1 check
    # Need to handle timezone parsing for publishedAt
    
    # Ensure publish_time is datetime
    # publishedAt from API is ISO format like 2023-10-27T10:00:00Z
    # We will treat them as UTC
    
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    
    hour1_data_list = []
    
    mask_monitoring = pending_df['status'] == 'monitoring'
    # We only iterate indices where mask is true
    
    for index, row in pending_df[mask_monitoring].iterrows():
        try:
            pub_time_str = row['publish_time']
            # Parse ISO format. Z indicates UTC.
            pub_time = isodate.parse_datetime(pub_time_str)
            
            # Helper to ensure both are offset-aware for subtraction
            if pub_time.tzinfo is None:
                pub_time = pub_time.replace(tzinfo=datetime.timezone.utc)
            
            time_diff = now_utc - pub_time
            
            # Check if 1 hour has passed (e.g., between 60 and 120 mins usually, 
            # but here condition is just >= 1 hour. We should grab it as soon as it crosses 1 hour)
            if time_diff.total_seconds() >= 3600:
                print(f"Fetching 1-hour stats for {row['video_id']} (Age: {time_diff})")
                
                # Fetch Stats
                response = youtube.videos().list(
                    part="statistics",
                    id=row['video_id']
                ).execute()
                
                items = response.get("items", [])
                if items:
                    stats = items[0]["statistics"]
                    
                    hour1_data_list.append({
                        "video_id": row['video_id'],
                        "hour1_views": int(stats.get("viewCount", 0)),
                        "hour1_likes": int(stats.get("likeCount", 0)),
                        "hour1_comments": int(stats.get("commentCount", 0))
                    })
                    
                    # Update status so we don't check hour 1 again, wait for final
                    pending_df.at[index, 'status'] = 'waiting_final'
                else:
                    # Video might have been deleted
                    print(f"Video {row['video_id']} not found (maybe deleted).")
                    pending_df.at[index, 'status'] = 'deleted'
        except Exception as e:
            print(f"Error processing hour 1 stats for {row['video_id']}: {e}")

    # Save collected data
    if hour1_data_list:
        h1_df = pd.DataFrame(hour1_data_list)
        save_csv(h1_df, HOUR1_DATA_FILE, mode='a', header=not os.path.exists(HOUR1_DATA_FILE))
        print(f"Saved {len(h1_df)} records to {HOUR1_DATA_FILE}")

    # Save state
    save_csv(pending_df, PENDING_VIDEOS_FILE, mode='w')

def collect_final_views():
    """
    Step 6:
    - Checks pending_videos.csv for videos in 'waiting_final' status.
    - If (current_time - publish_time) >= 7 days, fetch views.
    - Saves to final_data.csv.
    - Updates status to 'completed'.
    """
    print(f"[{datetime.datetime.now()}] Collecting Final Stats (7 Days)...")
    youtube = get_youtube_service()
    
    pending_df = load_csv(PENDING_VIDEOS_FILE)
    if pending_df.empty or 'status' not in pending_df.columns:
        return

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    final_data_list = []
    
    mask_waiting = pending_df['status'] == 'waiting_final'
    
    for index, row in pending_df[mask_waiting].iterrows():
        try:
            pub_time_str = row['publish_time']
            pub_time = isodate.parse_datetime(pub_time_str)
            if pub_time.tzinfo is None:
                pub_time = pub_time.replace(tzinfo=datetime.timezone.utc)
            
            time_diff = now_utc - pub_time
            
            # 7 days in seconds = 7 * 24 * 3600 = 604800
            if time_diff.total_seconds() >= 604800:
                print(f"Fetching final stats for {row['video_id']}")
                
                response = youtube.videos().list(
                    part="statistics",
                    id=row['video_id']
                ).execute()
                
                items = response.get("items", [])
                if items:
                    stats = items[0]["statistics"]
                    final_data_list.append({
                        "video_id": row['video_id'],
                        "final_views": int(stats.get("viewCount", 0))
                    })
                    pending_df.at[index, 'status'] = 'completed'
                else:
                     pending_df.at[index, 'status'] = 'deleted'

        except Exception as e:
            print(f"Error processing final stats for {row['video_id']}: {e}")

    if final_data_list:
        final_df = pd.DataFrame(final_data_list)
        save_csv(final_df, FINAL_DATA_FILE, mode='a', header=not os.path.exists(FINAL_DATA_FILE))
        print(f"Saved {len(final_df)} records to {FINAL_DATA_FILE}")
        
        # Trigger Merge whenever we have new final data
        merge_dataset()

    save_csv(pending_df, PENDING_VIDEOS_FILE, mode='w')

def merge_dataset():
    """
    Step 7:
    - Merges static_data, hour1_data, and final_data into dataset.csv.
    - Ensure logical consistency.
    """
    print(f"[{datetime.datetime.now()}] Merging Datasets...")
    
    # Load all fragments
    if not os.path.exists(STATIC_DATA_FILE) or \
       not os.path.exists(HOUR1_DATA_FILE) or \
       not os.path.exists(FINAL_DATA_FILE):
        print("Not all data files exist yet. Skipping merge.")
        return

    static_df = pd.read_csv(STATIC_DATA_FILE)
    
    # Robust check: Backfill 'category' if missing in older files
    if 'category' not in static_df.columns:
        print(f"Warning: 'category' column missing in {STATIC_DATA_FILE}. Backfilling with 'Unknown'.")
        static_df['category'] = 'Unknown'
        
    if 'duration_seconds' not in static_df.columns:
         print(f"Warning: 'duration_seconds' column missing in {STATIC_DATA_FILE}. Backfilling with 0.")
         static_df['duration_seconds'] = 0.0

    hour1_df = pd.read_csv(HOUR1_DATA_FILE)
    final_df = pd.read_csv(FINAL_DATA_FILE)

    if static_df.empty or hour1_df.empty or final_df.empty:
        print("One of the data files is empty. Skipping merge.")
        return

    # Merge Static + Hour1
    merged_df = pd.merge(static_df, hour1_df, on="video_id", how="inner")
    
    # Merge + Final
    merged_df = pd.merge(merged_df, final_df, on="video_id", how="inner")

    # Select final columns
    final_columns = [
        "video_id",
        "category",
        "hour1_views",
        "hour1_likes",
        "hour1_comments",
        "subscriber_count",
        "avg_last10_views",
        "duration_seconds",
        "final_views"
    ]
    
    # Save to dataset.csv
    # We overwrite dataset.csv each time to reflect the growing complete set
    merged_df = merged_df[final_columns]
    save_csv(merged_df, DATASET_FILE)
    print(f"Dataset updated. Total complete records: {len(merged_df)}")

# --- Main Scheduling Loop ---

def job_hourly():
    discover_videos()    # Finds and immediately gets static stats
    collect_hour1_stats() # Checks if any are ready for 1-hour check

def job_daily():
    collect_final_views()

def run_once():
    """Runs the pipeline jobs once and exits. Useful for GitHub Actions/Cron."""
    print("--- YouTube Virality Prediction Pipeline: Single Run ---")
    job_hourly()
    job_daily()
    merge_dataset() # Ensure dataset is merged on single runs
    print("Single execution completed.")

def run_pipeline():
    """Starts the persistent scheduling loop."""
    print("--- YouTube Virality Prediction Pipeline Started (Continuous Mode) ---")

    # Schedule
    schedule.every().hour.do(job_hourly)
    schedule.every().day.do(job_daily)
    
    # Run once immediately on start
    job_hourly()
    job_daily()

    while True:
        schedule.run_pending()
        time.sleep(60) # Watch every minute

if __name__ == "__main__":
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        run_once()
    else:
        run_pipeline()
