import os
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from apify_client import ApifyClient
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI")

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
    """
    if not APIFY_TOKEN:
        raise Exception("APIFY_TOKEN is missing in .env!")

    client_apify = ApifyClient(APIFY_TOKEN)

    run_input = {
        "videoUrls": [video_url],
    }

    try:
        run = client_apify.actor("scrape-creators/best-youtube-transcripts-scraper").call(run_input=run_input)
        
        text_parts = []
        dataset_items = client_apify.dataset(run["defaultDatasetId"]).iterate_items()
        
        for item in dataset_items:
            if "text" in item:
                text_parts.append(item["text"])
            elif "transcript" in item and isinstance(item["transcript"], list):
                for segment in item["transcript"]:
                    if "text" in segment:
                        text_parts.append(segment["text"])
            elif "text" in item.get("snippet", {}):
                 text_parts.append(item["snippet"]["text"])
        
        if not text_parts:
            return None
            
        return " ".join(text_parts)

    except Exception as e:
        print(f"  -> Apify Error: {e}")
        return None

def summarize_transcript(title: str, transcript: str):
    """Sends the transcript to OpenAI for summarization with direct narrative style."""
    if not OPENAI_API_KEY:
        print("  -> SKIP: OPENAI_API_KEY missing.")
        return None

    prompt = f"""
Analyze the following YouTube video transcript and provide a direct analysis in BOTH Hungarian (HU) and English (EN).
Video Title: {title}

Transcript:
{transcript[:30000]}

STYLE GUIDELINES (MANDATORY):
- Dive IMMEDIATELY into the facts and analysis. 
- NO INTROS: Never start with "A videó...", "Ez a videó...", "Ebben a részben...", "The video...", "In this video...", "This transcript...", etc.
- TONE: You are an expert analyst telling the reader exactly what is happening in the market and what the key takeaways are. 
- EXAMPLE OF BAD START: "A videó bemutatja az Nvidia legújabb..."
- EXAMPLE OF GOOD START: "Az Nvidia árfolyama brutális emelkedésbe kezdett a kínai export hírére..."

Return the result as a raw JSON object with the following keys:
{{
  "summary_hu": "8-12 mondatos elemző összefoglaló magyarul.",
  "summary_en": "8-12 sentence analytical summary in English.",
  "crypto_sentiment": "Bullish, Bearish, or Neutral regarding crypto markets (always in English).",
  "sentiment_score": 0-100 (Integer: 0 = extremely bearish, 100 = extremely bullish),
  "key_points_hu": ["Pont 1", "Pont 2", "Pont 3"],
  "key_points_en": ["Point 1", "Point 2", "Point 3"],
  "main_topics": ["Topic 1", "Topic 2"]
}}
"""
    max_retries = 3
    for attempt in range(max_retries):
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
            if "429" in str(e):
                wait_time = (2 ** attempt) * 10
                print(f"      Rate limited. Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            print(f"      Exception with OpenAI API: {e}")
            break
    return None

def get_videos_and_transcripts(youtube, channel_id, processed_ids, days_back=2):
    since = (datetime.now(timezone.utc) - timedelta(days=days_back)).isoformat().replace("+00:00", "Z")

    request = youtube.search().list(
        part="snippet",
        channelId=channel_id,
        publishedAfter=since,
        maxResults=10, 
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
                video_entry = {
                    "video_id": video_id,
                    "title": title,
                    "published_at": publish_raw,
                    "sort_date": publish_date,
                    "url": video_url,
                    "transcript": transcript_text
                }

                # SUMMARIZATION
                print(f"  -> Summarizing with AI...")
                summary_data = summarize_transcript(title, transcript_text)
                if summary_data:
                    video_entry.update(summary_data)
                    print("  -> SUCCESS: Summary generated.")
                
                new_data.append(video_entry)
                processed_ids.add(video_id)
                print("  -> SUCCESS: Transcript downloaded and summarized.")
            else:
                 print("  -> ERROR: Apify returned empty result (no transcript found?).")

        except Exception as e:
            print(f"  -> UNEXPECTED ERROR: {e}")
            
    return new_data

def check_and_fix_summaries(days_back=10):
    """
    Checks the last X days of data for any videos missing summaries and fixes them.
    Handles multiple videos per author per day.
    """
    print(f"\n--- Starting final summary check for the last {days_back} days ---")
    
    for i in range(days_back + 1):
        # We use local time for date folders as per script logic elsewhere
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
        folder_path = os.path.join("data", date_str)
        
        if not os.path.exists(folder_path):
            continue
            
        print(f" Checking {date_str}...")
        
        for filename in os.listdir(folder_path):
            if not filename.endswith(".json"):
                continue
                
            file_path = os.path.join(folder_path, filename)
            modified = False
            
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if not content.strip():
                        continue
                    videos = json.loads(content)
            except Exception as e:
                print(f"  -> Error reading {file_path}: {e}")
                continue
            
            if not isinstance(videos, list):
                continue
                
            for video in videos:
                # Check if summary is missing
                if "summary_hu" not in video or not video["summary_hu"]:
                    title = video.get("title", "Unknown Title")
                    transcript = video.get("transcript")
                    
                    if transcript:
                        print(f"  -> Missing summary for: {title} ({filename})")
                        summary_data = summarize_transcript(title, transcript)
                        if summary_data:
                            video.update(summary_data)
                            modified = True
                            print(f"     [+] Summary generated successfully.")
                        else:
                            print(f"     [!] Failed to generate summary.")
                    else:
                        print(f"  -> SKIP: No transcript for {title} (cannot summarize)")
            
            if modified:
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(videos, f, ensure_ascii=False, indent=4)
                print(f"  -> UPDATED: {file_path}")

    print("--- Final summary check completed ---\n")

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

    # FINAL CHECK: Ensure everything in last 10 days has summaries
    check_and_fix_summaries(days_back=10)

if __name__ == "__main__":
    main()
