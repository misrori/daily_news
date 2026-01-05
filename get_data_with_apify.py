import os
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from apify_client import ApifyClient
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")

CHANNELS = [
    "https://www.youtube.com/@IvanOnTech",
    "https://www.youtube.com/@alessiorastani",
    "https://www.youtube.com/@CoinBureau",
    "https://www.youtube.com/@coingecko",
    "https://www.youtube.com/@DataDispatch",
    "https://www.youtube.com/@FelixFriends",
    "https://www.youtube.com/@TomNashTV",
    "https://www.youtube.com/@DavidCarbutt"
]

HISTORY_FILE = os.path.join("data", "processed_videos.json")

def load_history():
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            try:
                return set(json.load(f))
            except json.JSONDecodeError:
                return set()
    return set()

def save_history(history_set):
    os.makedirs("data", exist_ok=True)
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(list(history_set), f, indent=4)

def get_channel_id(youtube, handle_url):
    handle = handle_url.split("/")[-1]
    request = youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1)
    response = request.execute()
    items = response.get("items", [])
    return items[0]["snippet"]["channelId"] if items else None

def get_transcript_via_apify(video_url):
    """
    Fetches transcript via Apify.
    FIX: Updated input format to use 'videoUrls' as requested by the Actor.
    """
    if not APIFY_TOKEN:
        raise Exception("APIFY_TOKEN is missing in .env!")

    client = ApifyClient(APIFY_TOKEN)

    # --- FIX IS HERE ---
    # The Actor requires 'videoUrls' (list of strings), not 'startUrls'.
    run_input = {
        "videoUrls": [video_url],
    }

    try:
        # Using the actor that triggered the error (assuming it exists and just needed correct input)
        # If this still fails with "Actor not found", we can switch to "gentle-rent/youtube-transcripts"
        run = client.actor("scrape-creators/best-youtube-transcripts-scraper").call(run_input=run_input)
        
        # Fetch results from the dataset
        text_parts = []
        dataset_items = client.dataset(run["defaultDatasetId"]).iterate_items()
        
        for item in dataset_items:
            # Logic to handle different output formats
            if "text" in item:
                text_parts.append(item["text"])
            elif "transcript" in item and isinstance(item["transcript"], list):
                for segment in item["transcript"]:
                    if "text" in segment:
                        text_parts.append(segment["text"])
            elif "text" in item.get("snippet", {}): # Sometimes it's nested
                 text_parts.append(item["snippet"]["text"])
        
        if not text_parts:
            return None
            
        return " ".join(text_parts)

    except Exception as e:
        print(f"  -> Apify Error: {e}")
        return None

def get_videos_and_transcripts(youtube, channel_id, processed_ids, days_back=30):
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat().replace("+00:00", "Z")

    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        publishedAfter=since,
        maxResults=50, 
        order="date",
        type="video"
    )
    response = request.execute()
    video_items = response.get("items", [])
    
    new_data = []

    for item in video_items:
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        publish_raw = item["snippet"]["publishedAt"]
        publish_date = publish_raw.split("T")[0]
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if video_id in processed_ids:
            print(f"SKIPPING ({publish_date}): {title}")
            continue
        
        print(f"Processing [{publish_date}]: {title}")
        
        try:
            transcript_text = get_transcript_via_apify(video_url)
            
            if transcript_text:
                new_data.append({
                    "video_id": video_id,
                    "title": title,
                    "published_at": publish_raw,
                    "sort_date": publish_date,
                    "url": video_url,
                    "transcript": transcript_text
                })
                
                processed_ids.add(video_id)
                print("  -> SUCCESS: Transcript downloaded (Apify).")
            else:
                 print("  -> ERROR: Apify returned empty result (no transcript found?).")

        except Exception as e:
            print(f"  -> UNEXPECTED ERROR: {e}")
            
    return new_data

def main():
    if not API_KEY:
        print("ERROR: YOUTUBE_API_KEY is missing!")
        return

    youtube = build("youtube", "v3", developerKey=API_KEY)
    processed_ids = load_history()
    original_count = len(processed_ids)
    
    for url in CHANNELS:
        print(f"\n--- Checking channel: {url} ---")
        channel_id = get_channel_id(youtube, url)
        if not channel_id: continue
        
        channel_name = url.split("@")[-1]
        
        videos = get_videos_and_transcripts(youtube, channel_id, processed_ids, days_back=14)
        
        if not videos:
            print("No new videos to save.")
            continue

        videos_by_date = defaultdict(list)
        for v in videos:
            videos_by_date[v['sort_date']].append(v)
        
        for date_key, video_list in videos_by_date.items():
            folder_path = os.path.join("data", date_key)
            os.makedirs(folder_path, exist_ok=True)
            file_path = os.path.join(folder_path, f"{channel_name}.json")
            
            existing_data = []
            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                    except:
                        existing_data = []
            
            final_data = existing_data + video_list
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(final_data, f, ensure_ascii=False, indent=4)
            print(f" >> Saved: {file_path}")

    if len(processed_ids) > original_count:
        save_history(processed_ids)
        print("\nHistory updated.")
    else:
        print("\nHistory unchanged.")

if __name__ == "__main__":
    main()