import os
import requests
import pandas as pd
from dotenv import load_dotenv
# Your YouTube API Key
load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # Replace with your channel ID
COMMENTS_LIMIT = 5  # Max comments per request (100 is API limit)

def get_channel_videos(api_key, channel_id):
    """Fetch all video IDs from a YouTube channel."""
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "id",
        "channelId": channel_id,
        "maxResults": 50,  # Max is 50 per request, implement pagination if needed
        "order": "date",  # Sort videos by newest first
        "type": "video",
        "key": api_key
    }

    video_ids = []
    next_page_token = None

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        video_ids.extend([item["id"]["videoId"] for item in data.get("items", [])])
        
        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break  # Stop if no more pages

    return video_ids

def get_video_comments(api_key, video_id, max_results=COMMENTS_LIMIT):
    """Fetch comments from a specific video."""
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": max_results
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    comments_data = response.json().get("items", [])

    comments_list = []
    for comment in comments_data:
        snippet = comment["snippet"]["topLevelComment"]["snippet"]
        comments_list.append({
            "Video ID": video_id,
            "Author": snippet["authorDisplayName"],
            "Comment": snippet["textDisplay"],
            "Likes": snippet["likeCount"],
            "Published At": snippet["publishedAt"]
        })

    return comments_list

def scrape_channel_comments(api_key, channel_id):
    """Fetch comments from all videos of a channel."""
    video_ids = get_channel_videos(api_key, channel_id)
    all_comments = []

    print(f"Fetching comments from {len(video_ids)} videos...")

    for video_id in video_ids:
        try:
            comments = get_video_comments(api_key, video_id)
            all_comments.extend(comments)
        except Exception as e:
            print(f"Error fetching comments for video {video_id}: {e}")

    return pd.DataFrame(all_comments)

# Run the script
df_comments = scrape_channel_comments(API_KEY, CHANNEL_ID)

# Save to CSV
output_file = "youtube_channel_comments.csv"
df_comments.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Comments saved to {output_file}")
