import json
import pandas as pd
import boto3
from io import StringIO

from googleapiclient.discovery import build

# 1. Initialize API
API_KEY = '###YOUR_API_KEY###'
youtube = build("youtube", "v3", developerKey=API_KEY)
channel_id = "UCX6OQ3DkcsbYNE6H8uQQuVA"

# 2. Get Channel Response
channel_response = youtube.channels().list(
    part="snippet,statistics",
    id=channel_id
).execute()

# 3. Function: Extract Channel Info
def extract_channel(response):
    channel_list = []
    try:
        for data in response['items']:
            channel_dict = {
                'Channel_ID': data['id'],
                'Channel_Name': data['snippet']['title'],
                'Total_Views': data['statistics']['viewCount'],
                'Total_Subs': data['statistics']['subscriberCount'],
                'Total_Videos': data['statistics']['videoCount'],
                'Country': data['snippet'].get('country', 'N/A')
            }
            channel_list.append(channel_dict)
        return pd.DataFrame(channel_list)
    except Exception as e:
        print("Error extracting channel data:", e)
        

# 4. Get Videos Uploaded By Channel
                
video_response = youtube.search().list(
    part="snippet",
    channelId=channel_id,
    type="video",
    order="date",
    maxResults=50
).execute()

# 5. Function: Extract Videos & Stats
def extract_video_stats(video_response):
    try:
        video_list = []
        for item in video_response['items']:
            
            vidz = {
                'Channel_ID': item['snippet']['channelId'],
                'Video_ID': item['id']['videoId'],
                'Title': item['snippet']['title'],
                'Published_Date': item['snippet']['publishedAt'][:10]
            }
            video_list.append(vidz)

        only_ids = []
        for vid in video_list:
            only_ids.append(vid['Video_ID'])


        video_stats_response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(only_ids)
        ).execute()

        stats_list = []
        for data in video_stats_response['items']:
            video_dict = {
                'Video_ID': data['id'],
                'Video_Views': data['statistics'].get('viewCount', '0'),
                'Video_Likes': data['statistics'].get('likeCount', '0'),
                'Total_Comments': data['statistics'].get('commentCount', '0')
            }
            stats_list.append(video_dict)

        video_list =  pd.DataFrame(video_list)
        stats_list =  pd.DataFrame(stats_list)

        video_stats_join = video_list.merge(stats_list, on = 'Video_ID', how = 'left')
        return pd.DataFrame(video_stats_join)

    except Exception as e: 
        print('Error extracting video stats:', e)
        
 # function stored in varibles       
channel_s3 = extract_channel(channel_response)
video_stats_s3 = extract_video_stats(video_response)


#uploading to S3 Bucket

bucket = 'taha-aws'
file_list = {'Staging/channels.csv': channel_s3,
             'Staging/video_stats.csv':video_stats_s3}

def s3_uploader():
    try:
        for key, df in file_list.items():
            s3 = boto3.client('s3')
            memory = StringIO()
            df.to_csv(memory, index = False)
            memory.seek(0)
            s3.put_object(Bucket=bucket, Key=key, Body=memory.getvalue())
            print(f'File {key[8:]} uploaded to  {bucket}')
    except Exception as e:
        print('Check ',e)

s3_uploader()
