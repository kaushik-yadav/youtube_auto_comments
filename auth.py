import os
import google.auth
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# defines the required scopes (mainly used to like and comment)
scopes = ["https://www.googleapis.com/auth/youtube.force-ssl"]

def authenticate_youtube():
    flow = InstalledAppFlow.from_client_secrets_file(
        "client_secret.json", scopes
    )
    credentials = flow.run_local_server(port=0)
    
    youtube = build("youtube", "v3", credentials=credentials)
    return youtube
