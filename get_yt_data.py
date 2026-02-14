import os
import requests
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta
from googleapiclient.discovery import build
import yt_dlp
from openai import OpenAI


load_dotenv()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_KEY_2 = os.getenv("YOUTUBE_API_KEY_2")
OPENAI_API_KEY = os.getenv("OPENAI")
client = OpenAI(api_key=OPENAI_API_KEY)
MODEL_NAME = "gpt-4o-mini"


youtube_chanel_list = [{'name':'Ivan On Tech', 'id':'UCrYmtJBtLdtm2ov84ulV-yg', 'handle':'ivanontech'}, 
                       {'name': 'Alessio Rastani', 'id':'UCnJjRjmthxPCoQaAL44tR6g', 'handle':'alessio'}
                       ]



def get_english_transcript(url):
    ydl_opts = {
        "skip_download": True,
        "writeautomaticsub": True,
        "writesubtitles": True,
        "subtitleslangs": ["en"],
        "subtitlesformat": "json3",
        "quiet": True,
        "extractor_args": {
        "youtube": {
            "player_client": ["android"]
            }
        }
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

        duration_seconds = info.get("duration", 0)
        duration_minutes = duration_seconds / 60

        if duration_minutes < 5:
            return "rovid"

        # először a manuális felirat
        subtitles = info.get("subtitles", {}).get("en")

        # ha nincs, akkor az automatikus
        if not subtitles:
            subtitles = info.get("automatic_captions", {}).get("en")

        if not subtitles:
            return None

        sub_url = subtitles[0]["url"]
        data = requests.get(sub_url).json()

        text_parts = []
        for event in data.get("events", []):
            if "segs" in event:
                for seg in event["segs"]:
                    text_parts.append(seg["utf8"])

        return " ".join(text_parts).replace("\n", " ").strip()

def get_recent_videos(channel_id, hours=120):
    try:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)

        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat("T") + "Z"

        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            publishedAfter=since,
            maxResults=10,
            order="date",
            type="video"
        )
        response = request.execute()

        return response["items"]

    except:
        youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY_2)

        since = (datetime.utcnow() - timedelta(hours=hours)).isoformat("T") + "Z"

        request = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            publishedAfter=since,
            maxResults=10,
            order="date",
            type="video"
        )
        response = request.execute()

        return response["items"]



    youtube = build("youtube", "v3", developerKey=api_key)

    request = youtube.search().list(
        part="snippet",
        q=handle,
        type="channel",
        maxResults=1
    )
    response = request.execute()
    if response.code != 200:
        print(f"Error fetching channel ID for handle '{handle}': {response.text}")
    

    items = response.get("items", [])
    if not items:
        return None

    return items[0]["snippet"]["channelId"]


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




for channel in youtube_chanel_list:
    print(channel['id'])
    last_videos = get_recent_videos(channel['id'])  
    # log message
    print('\n\n\n')
    print(f"Processing channel: {channel['name']}")
    print(f"Total videos: {len(last_videos)}")


    for video in last_videos:

        #print(video['snippet']['title'])
        # if short video skipp
        if '#shorts' in video['snippet']['title']:
            print (f"{video['snippet']['title']} shorts video I skipp!! \n")

        else:
            #check if processed
            os.makedirs('data', exist_ok=True)
            os.makedirs(os.path.join('data', video['snippet']['publishedAt'].split('T')[0]), exist_ok=True)


            file_path = os.path.join('data', video['snippet']['publishedAt'].split('T')[0], f"{channel['handle']}_{video['id']['videoId']}.json")

            if os.path.exists(file_path):
                print(f"SKIPPING: ALREADY processed {video['snippet']['title']}")
            else:
                try:
                    print('---------------------------------------------------------')
                    print(f"Processing:  {video['snippet']['title']} with {video['id']}\n")
                    transcript = get_english_transcript(f"https://www.youtube.com/watch?v={video['id']['videoId']}")

                    if transcript and transcript!='rovid':
                        video_json_data = {
                            "channel_name": channel['name'],
                            "channel_id": channel['id'],
                            "channel_handle": channel['handle'],
                            "video_id": video['id']['videoId'],
                            "title": video['snippet']['title'],
                            "published_at": video['snippet']['publishedAt'],
                            "url": f"https://www.youtube.com/watch?v={video['id']}",
                            "transcript": transcript,
                            "sort_data": video['snippet']['publishedAt'].split('T')[0]
                        }
            
                        print('summarize with ai')

                        summary_data = summarize_transcript(video_json_data['title'], video_json_data['transcript'])
                        video_json_data.update(summary_data)
                        print(video_json_data)
                        #save to file
                        with open(file_path, 'a', encoding='utf-8') as f:
                            json.dump(video_json_data, f, ensure_ascii=False, indent=4)
                except Exception as e:
                    print(f"Error: {e}")
                    continue

                        
                    
                else:
                    print("Transcript not available")
                

            
