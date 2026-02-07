import os
import json
import time
import argparse
from typing import List, Dict
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Using gpt-4o-mini as the requested "nano"-like model
MODEL_NAME = "gpt-4o-mini"

def summarize_transcript(title: str, transcript: str) -> Dict:
    """Sends the transcript to OpenAI for summarization with direct narrative style."""
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
  "summary_hu": "3-5 mondatos elemző összefoglaló magyarul.",
  "summary_en": "3-5 sentence analytical summary in English.",
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

def process_directory(directory: str, force: bool):
    """Processes all JSON files in the given directory."""
    if not os.path.exists(directory):
        print(f"Directory {directory} does not exist.")
        return

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            file_path = os.path.join(directory, filename)
            print(f"Processing {filename}...")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            except Exception as e:
                print(f"  Error reading {filename}: {e}")
                continue
            
            updated = False
            for video in data:
                # Force update to get the new narrative style and ensure sentiment_score is accurate
                hu_summary = video.get("summary_hu", "").lower()
                en_summary = video.get("summary_en", "").lower()
                
                bad_starts = ["a videó", "ez a videó", "ebben a videó", "the video", "this video", "in this video"]
                has_bad_start = any(hu_summary.startswith(s) for s in bad_starts) or any(en_summary.startswith(s) for s in bad_starts)
                
                if force or "sentiment_score" not in video or has_bad_start:
                    print(f"  Summarizing/Refining (Narrative Style): {video['title']}")
                    summary_data = summarize_transcript(video.get('title', 'Unknown'), video.get('transcript', ''))
                    if summary_data:
                        video.update(summary_data)
                        updated = True
                        print(f"    Successfully updated.")
                        time.sleep(1) # Small delay
                    else:
                        print(f"    Failed to get summary for {video['title']}")
            
            if updated:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                print(f"  Updated {filename}")
            else:
                print(f"  No changes for {filename}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize YouTube transcripts using OpenAI API.")
    parser.add_argument("--dir", type=str, required=True, help="Directory containing JSON files.")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing summaries.")
    
    args = parser.parse_args()
    process_directory(args.dir, args.force)
