import os
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
# Fontos: A te verziód szerint importáljuk, feltételezve, hogy a library támogatja a fetch/objektum modellt
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")

CHANNELS = [
    "https://www.youtube.com/@IvanOnTech",

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

def get_videos_and_transcripts(youtube, channel_id, processed_ids, days_back=30):
    """
    Lekéri a videókat az elmúlt X napból a te működő transcript logikáddal.
    """
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

    # 1. Instantiate the class (A te kódod alapján)
    yt_api = YouTubeTranscriptApi()

    for item in video_items:
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        publish_raw = item["snippet"]["publishedAt"]
        publish_date = publish_raw.split("T")[0]

        if video_id in processed_ids:
            print(f"SKIPPING ({publish_date}): {title}")
            continue
        
        print(f"Feldolgozás [{publish_date}]: {title}")
        
        transcript_text = "N/A"
        
        # --- A TE MŰKÖDŐ KÓDOD BEILLESZTVE ---
            # Fetch the transcript (returns a list of objects)
        transcript_list = yt_api.fetch(video_id)

            # NEW WAY (Dot notation):
        transcript_text = " ".join([entry.text for entry in transcript_list])
            
  

        new_data.append({
            "video_id": video_id,
            "title": title,
            "published_at": publish_raw,
            "sort_date": publish_date,
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "transcript": transcript_text
        })
        
        processed_ids.add(video_id)
            
    return new_data

def main():
    if not API_KEY:
        print("HIBA: Nincs YOUTUBE_API_KEY!")
        return

    youtube = build("youtube", "v3", developerKey=API_KEY)
    processed_ids = load_history()
    original_count = len(processed_ids)
    
    for url in CHANNELS:
        print(f"\n--- Csatorna vizsgálata: {url} ---")
        channel_id = get_channel_id(youtube, url)
        if not channel_id: continue
        
        channel_name = url.split("@")[-1]
        
        # 14 napos visszatekintés
        videos = get_videos_and_transcripts(youtube, channel_id, processed_ids, days_back=14)
        
        if not videos:
            print("Nincs új mentendő videó.")
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
            
            print(f" >> Mentve: {file_path} ({len(video_list)} új videó)")

    if len(processed_ids) > original_count:
        save_history(processed_ids)
        print("\nHistory frissítve.")

if __name__ == "__main__":
    main()
