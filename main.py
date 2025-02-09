import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base,sessionmaker

# Your YouTube API Key
load_dotenv()
API_KEY = os.getenv("API_KEY")
CHANNEL_ID = os.getenv("CHANNEL_ID")  # Replace with your channel ID
COMMENTS_LIMIT = 10

# Initializing the database
Base = declarative_base()

class YoutubeComments(Base):
    ## this is part of orm in sqlalchemy which tells that this class maps to a table in database name __tablename__
    __tablename__ = "youtube_comments"
    ## now we provide the columns in the db
    # we make a primary key named id with autoincrement
    id = Column(Integer,primary_key = True, autoincrement=True)
    video_id = Column(String,nullable = False)
    author = Column(String,nullable = False)
    comment = Column(String,nullable = False)
    likes = Column(Integer,nullable = False)
    published_at = Column(DateTime,nullable = False)

## setting up the sqlite database
    
engine = create_engine("sqlite:///youtube_comments.db")
Base.metadata.create_all(engine)
# binding the session with our db engine(managing connection to our db)
Session = sessionmaker(bind = engine)
session = Session()

def get_channel_video_ids(api_key, channel_id):
    # Extracts video ids of a channel
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
    # Fetch comments from a video
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
        cleaned_date = snippet["publishedAt"][:-1].replace("T"," ")
        date_object = datetime.strptime(cleaned_date, "%Y-%m-%d %H:%M:%S")
        comments_list.append({
            "video_id": video_id,
            "author": snippet["authorDisplayName"],
            "comment": snippet["textDisplay"],
            "likes": snippet["likeCount"],
            "published_at": date_object
        })

    return comments_list

def store_comments(comments_list):
    for comment in comments_list:
        # creating an object of comments by mapping them to class YoutubeComments and further appending the comments as columns in db

        comment_obj = YoutubeComments(
            video_id = comment["video_id"],
            author = comment["author"],
            comment = comment["comment"],
            likes = comment["likes"],
            published_at = comment["published_at"]
        )

        ## adding the comment obj into our session

        session.add(comment_obj)
    # committing all the changes saved in our session
    session.commit() 

def main():
    # Fetch comments from a channel
    video_ids = get_channel_video_ids(API_KEY,CHANNEL_ID)

    print(f"Fetching comments from {len(video_ids)} videos...")

    for video_id in video_ids:
        try:
            comments = get_video_comments(API_KEY, video_id)
            if comments and comments[0]["comment"]:
                store_comments(comments)
            #all_comments.extend(comments)
        except Exception as e:
            print(f"Error fetching comments for video https://www.youtube.com/watch?v={video_id}: {e}")
    print("All data successfully stored in database")
    # if you want csv
    # return all_comments

if __name__ == "__main__":
    main()

## if you want the data in a csv :
#df_comments = main(API_KEY, CHANNEL_ID)
# Save to CSV
#output_file = "youtube_channel_comments.csv"
#df_comments.to_csv(output_file, index=False, encoding="utf-8-sig")
#print(f"Comments saved to {output_file}")
