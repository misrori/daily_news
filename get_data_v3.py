import os
import json
import time
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# API KEYS
YOUTUBE_API_KEYS = [
    os.getenv("YOUTUBE_API_KEY"),
    os.getenv("YOUTUBE_API_KEY_2") # User mentioned "two" keys
]
# Remove None values
YOUTUBE_API_KEYS = [k for k in YOUTUBE_API_KEYS if k]

OPENAI_API_KEY = os.getenv("OPENAI") or os.getenv("OPENAI_API_KEY")

client = OpenAI(api_key=OPENAI_API_KEY)
MODEL_NAME = "gpt-4o-mini"

CHANNELS = [
    "https://www.youtube.com/@IvanOnTech",
    "https://www.youtube.com/@alessiorastani",
    "https://www.youtube.com/@CoinBureau",
    "https://www.youtube.com/@coingecko",
    "https://www.youtube.com/@DataDispatch",
    "https://www.youtube.com/@FelixFriends",
    "https://www.youtube.com/@TomNashTV",
    "https://www.youtube.com/@DavidCarbutt",
    "https://www.youtube.com/@CTOLARSSON",
    "https://www.youtube.com/@elliotrades_official"
]

HISTORY_FILE = os.path.join("data", "processed_videos_v3.json")

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

def get_youtube_client(key_index=0):
    if key_index >= len(YOUTUBE_API_KEYS):
        return None
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEYS[key_index])

def get_channel_id(youtube, handle_url):
    handle = handle_url.split("/")[-1]
    try:
        request = youtube.search().list(part="snippet", q=handle, type="channel", maxResults=1)
        response = request.execute()
        items = response.get("items", [])
        return items[0]["snippet"]["channelId"] if items else None
    except Exception as e:
        print(f"Error getting channel ID for {handle_url}: {e}")
        return None

def get_transcript(video_id):
    """Fetches transcript using youtube_transcript_api."""
    try:
        # Based on get_data.py usage if it works there
        # Regular usage is usually YouTubeTranscriptApi.get_transcript(video_id)
        # But user's get_data.py has: yt_api = YouTubeTranscriptApi(); yt_api.fetch(video_id)
        # We'll try the standard static method first as it's more robust across versions
        try:
            transcript_list = YouTubeTranscriptApi.get_transcript(video_id)
            return " ".join([entry['text'] for entry in transcript_list])
        except:
            # Fallback to the style in get_data.py if it's a specific wrapper
            from youtube_transcript_api import YouTubeTranscriptApi as YTApiClass
            yt_api = YTApiClass()
            transcript_list = yt_api.fetch(video_id)
            return " ".join([entry.text for entry in transcript_list])
    except Exception as e:
        # print(f"  -> Transcript Error for {video_id}: {e}")
        return None

def summarize_transcript(title: str, transcript: str):
    """Sends the transcript to OpenAI for summarization."""
    if not OPENAI_API_KEY:
        return None

    prompt = f"""
Analyze the following YouTube video transcript and provide a direct analysis in BOTH Hungarian (HU) and English (EN).
Video Title: {title}

Transcript:
{transcript[:30000]}

STYLE GUIDELINES (MANDATORY):
- Dive IMMEDIATELY into the facts and analysis. 
- NO INTROS: Never start with "A videó...", "The video...", etc.
- TONE: You are an expert analyst.

Return the result as a raw JSON object:
{{
  "summary_hu": "8-12 mondatos elemző összefoglaló magyarul.",
  "summary_en": "8-12 sentence analytical summary in English.",
  "crypto_sentiment": "Bullish, Bearish, or Neutral",
  "sentiment_score": 0-100,
  "key_points_hu": ["Pont 1", "Pont 2", "Pont 3"],
  "key_points_en": ["Point 1", "Point 2", "Point 3"],
  "main_topics": ["Topic 1", "Topic 2"]
}}
"""
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "You are a direct, analytical narrator who outputs only valid JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        return json.loads(content)
    except Exception as e:
        print(f"      OpenAI Error: {e}")
        return None

def get_videos_and_transcripts(youtube, channel_id, processed_ids, hours_back=30):
    since = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat().replace("+00:00", "Z")

    try:
        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            publishedAfter=since,
            maxResults=15, 
            order="date",
            type="video"
        )
        response = request.execute()
    except Exception as e:
        print(f"  -> YouTube API Error: {e}")
        return [], True # Return True to indicate potential quota error

    video_items = response.get("items", [])
    new_data = []

    for item in video_items:
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        publish_raw = item["snippet"]["publishedAt"]
        publish_date = publish_raw.split("T")[0]
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        if video_id in processed_ids:
            continue

        if "#shorts" in title.lower():
            continue
        
        print(f"Processing [{publish_date}]: {title}")
        
        transcript_text = get_transcript(video_id)
        if not transcript_text:
            print(f"  -> No transcript found. Skipping.")
            continue

        print(f"  -> Summarizing with AI...")
        summary_data = summarize_transcript(title, transcript_text)
        
        if summary_data:
            video_entry = {
                "video_id": video_id,
                "title": title,
                "published_at": publish_raw,
                "sort_date": publish_date,
                "url": video_url,
                "transcript": transcript_text
            }
            video_entry.update(summary_data)
            new_data.append(video_entry)
            processed_ids.add(video_id)
            print("  -> SUCCESS: Saved with summary.")
        else:
            print("  -> ERROR: Summary failed. Skipping save.")
            
    return new_data, False

def main():
    if not YOUTUBE_API_KEYS:
        print("ERROR: YOUTUBE_API_KEY is missing!")
        return

    processed_ids = load_history()
    original_count = len(processed_ids)
    
    current_key_index = 0
    youtube = get_youtube_client(current_key_index)

    for url in CHANNELS:
        print(f"\n--- Checking channel: {url} ---")
        
        channel_id = get_channel_id(youtube, url)
        
        # Simple Quota rotation if channel_id fails or later search fails
        if not channel_id:
            if current_key_index + 1 < len(YOUTUBE_API_KEYS):
                current_key_index += 1
                print(f"Switching to API Key #{current_key_index + 1}")
                youtube = get_youtube_client(current_key_index)
                channel_id = get_channel_id(youtube, url)
            
        if not channel_id:
            print(f"Could not get channel ID for {url}")
            continue
        
        channel_name = url.split("@")[-1]
        
        videos, quota_error = get_videos_and_transcripts(youtube, channel_id, processed_ids, hours_back=30)
        
        if quota_error and current_key_index + 1 < len(YOUTUBE_API_KEYS):
            current_key_index += 1
            print(f"Quota error. Switching to API Key #{current_key_index + 1}")
            youtube = get_youtube_client(current_key_index)
            videos, _ = get_videos_and_transcripts(youtube, channel_id, processed_ids, hours_back=30)

        if not videos:
            continue

        # Group by date and save
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
