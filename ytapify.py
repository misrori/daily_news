import os
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
# CSAK ez az egy library kell
from apify_client import ApifyClient
from dotenv import load_dotenv
import dateutil.parser # A dátumok könnyebb kezeléséhez (pip install python-dateutil)

load_dotenv()
# Már csak ez az egy kulcs kell!
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

def get_channel_videos_apify(client, channel_url, max_results=20):
    """
    1. LÉPÉS: Lekéri a csatorna legfrissebb videóinak listáját.
    Ez sokkal olcsóbb ("Data Compute Unit"-ban), mint rögtön transcriptet kérni.
    """
    print(f"  -> Lista lekérése Apify-tól: {channel_url}...")
    
    run_input = {
        "startUrls": [{"url": channel_url}],
        "maxResults": max_results, # Elég az utolsó 20 videót csekkolni
        "downloadSubtitles": False, # Most még nem kell felirat, csak a lista
        "saveSubsToKvs": False,
    }

    try:
        # A 'streamers/youtube-scraper' nagyon megbízható a listázáshoz
        run = client.actor("streamers/youtube-scraper").call(run_input=run_input)
        
        videos = []
        # Végigiterálunk az eredményeken
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            if "id" in item and "title" in item:
                videos.append(item)
        return videos

    except Exception as e:
        print(f"  -> Apify Lista Hiba: {e}")
        return []

def get_transcript_apify(client, video_url):
    """
    2. LÉPÉS: Konkrét videó feliratának letöltése.
    """
    run_input = {
        "videoUrls": [video_url], # Figyelj, itt 'videoUrls' a kulcs!
    }

    try:
        # A korábban bevált transcript scraper
        run = client.actor("scrape-creators/best-youtube-transcripts-scraper").call(run_input=run_input)
        
        text_parts = []
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            if "text" in item:
                text_parts.append(item["text"])
            elif "transcript" in item and isinstance(item["transcript"], list):
                for segment in item["transcript"]:
                    if "text" in segment:
                        text_parts.append(segment["text"])
        
        if not text_parts:
            return None
            
        return " ".join(text_parts)

    except Exception as e:
        print(f"  -> Apify Transcript Hiba: {e}")
        return None

def parse_apify_date(date_str):
    """
    Az Apify néha fura formátumban adja a dátumot, ez segít parse-olni.
    """
    if not date_str:
        return datetime.now()
    try:
        # A dateutil.parser nagyon okos, szinte bármit felismer (ISO, emberi, stb.)
        return dateutil.parser.parse(date_str)
    except:
        return datetime.now()

def main():
    if not APIFY_TOKEN:
        print("HIBA: Nincs APIFY_TOKEN az .env fájlban!")
        return

    client = ApifyClient(APIFY_TOKEN)
    processed_ids = load_history()
    original_count = len(processed_ids)
    
    # Időablak (pl. elmúlt 14 nap)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=30)

    for channel_url in CHANNELS:
        channel_name = channel_name = channel_url.split("@")[-1]
        print(f"\n--- Csatorna feldolgozása: {channel_name} ---")
        
        # 1. Lekérjük a videók listáját (ez gyors)
        video_list_items = get_channel_videos_apify(client, channel_url)
        
        if not video_list_items:
            print("  -> Nem találtunk videókat (vagy hiba történt).")
            continue

        new_videos_to_save = []
        
        for item in video_list_items:
            video_id = item.get("id")
            title = item.get("title")
            video_url = item.get("url")
            date_str = item.get("date") # Az Apify gyakran "date" mezőbe teszi az ISO stringet
            
            # Ellenőrzés, hogy már feldolgoztuk-e
            if video_id in processed_ids:
                continue

            # Dátum ellenőrzés
            pub_date_obj = parse_apify_date(date_str)
            # Biztosítjuk, hogy offset-aware legyen az összehasonlításhoz
            if pub_date_obj.tzinfo is None:
                pub_date_obj = pub_date_obj.replace(tzinfo=timezone.utc)
            
            if pub_date_obj < cutoff_date:
                print(f"SKIPPING (Too old): {title}")
                continue
            
            # Formázott dátum a mappához (YYYY-MM-DD)
            sort_date = pub_date_obj.strftime("%Y-%m-%d")
            
            print(f"Feldolgozás [{sort_date}]: {title}")

            # 2. Ha új és időben van, lekérjük a transcriptet
            transcript_text = get_transcript_apify(client, video_url)
            
            if transcript_text:
                new_videos_to_save.append({
                    "video_id": video_id,
                    "title": title,
                    "published_at": date_str,
                    "sort_date": sort_date,
                    "url": video_url,
                    "views": item.get("viewCount", 0), # Extra adat, amit az Apify ad!
                    "duration": item.get("duration", "N/A"), # Extra adat!
                    "transcript": transcript_text
                })
                processed_ids.add(video_id)
                print("  -> SIKER: Transcript lementve.")
            else:
                print("  -> HIBA: Nincs transcript, kihagyjuk.")

        # Mentés fájlokba
        if new_videos_to_save:
            videos_by_date = defaultdict(list)
            for v in new_videos_to_save:
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
                print(f" >> Fájl frissítve: {file_path}")

    # History mentése
    if len(processed_ids) > original_count:
        save_history(processed_ids)
        print("\nHistory frissítve.")
    else:
        print("\nNincs új mentett videó.")

if __name__ == "__main__":
    main()
