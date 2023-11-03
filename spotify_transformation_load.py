import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def album_data(data):
    album_data_list=[]
    for row in data["items"]:
        album_id = row["track"]["album"]["id"]
        album_name = row["track"]["album"]["name"]
        album_release_date = row["track"]["album"]["release_date"]
        album_total_tracks = row["track"]["album"]["total_tracks"]
        album_external_urls = row["track"]["album"]["external_urls"]["spotify"]
        album_data = {"album_id":album_id, "album_name":album_name, "album_release_date":album_release_date,
                      "album_total_tracks":album_total_tracks, "album_external_urls":album_external_urls}
        album_data_list.append(album_data)
    return album_data_list

def artists_data(data):
    artist_data_list=[]
    for row in data["items"]:
        for key,value in row.items():
            if key == "track":
                for artist in value['artists']:
                    artist_id = artist["id"]
                    artist_name = artist["name"]
                    artist_external_urls = artist["external_urls"]["spotify"]
                    artist_data = {"artist_id":artist_id, "artist_name":artist_name, "artist_external_urls":artist_external_urls}
                    artist_data_list.append(artist_data)
    return artist_data_list
    
def tracks_data(data):
    track_data_list=[]
    for row in data["items"]:
        track_id = row["track"]["id"]
        track_name = row["track"]["name"]
        track_popularity = row["track"]["popularity"]
        track_duration =row["track"]["duration_ms"]
        track_number = row["track"]["track_number"]
        track_preview_url = row["track"]["preview_url"]
        track_external_urls = row["track"]["external_urls"]["spotify"]
        album_id = row["track"]["album"]["id"]
        artist_id = row["track"]["album"]["artists"][0]["id"]
        track_added_at = row["added_at"]
        track_data = {"track_id":artist_id, "track_name":track_name, "track_popularity":track_popularity, "track_duration": track_duration,
                         "track_number": track_number, "track_preview_url":track_preview_url, "track_external_urls":track_external_urls,
                        "album_id":album_id, "artist_id": artist_id, "track_added_at": track_added_at }
        track_data_list.append(track_data)
    return track_data_list

def lambda_handler(event, context):

    s3 = boto3.client('s3')
    bucket = 'spotify-etl-pipeline-96'  
    key = 'Raw_Data/Before_Processed_data/'
    # Retrieve a list of files in the S3 bucket with a specific prefix
    file_list = s3.list_objects(Bucket=bucket, Prefix=key)['Contents']
    
    spoitfy_file_data = []
    spotify_file_names = []
    
    for file in file_list:
         file = file['Key']
         # Check if the file has a JSON extension
         if file.split('.')[-1] == 'json':
            # Retrieve and load the JSON data from S3, then append it to the respective lists.
             response = s3.get_object(Bucket=bucket, Key= file)
             content = response['Body']
             jsonObject = json.loads(content.read())
             spoitfy_file_data.append(jsonObject)
             spotify_file_names.append(file)
    
    for data in spoitfy_file_data:
        # Extract and transform album, artist, and track data from each JSON.
        album_list = album_data(data)
        artist_list = artists_data(data)
        tracks_list = tracks_data(data)
        
        # Create DataFrames from the extracted data and perform data transformations.
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
        
        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        
        track_df = pd.DataFrame.from_dict(tracks_list)
        track_df = track_df.drop_duplicates(subset=['track_id'])
        track_df_copy = track_df.copy()
        track_df_copy['track_added_at'] = pd.to_datetime(track_df_copy['track_added_at'])
        track_df = track_df_copy
        
        # Upload the transformed data to the specified S3 locations.
        album_key = 'Transformed_Data/Album_Data/album_list' + str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=album_key, Body=album_content)
        
        artist_key = 'Transformed_Data/Artists_Data/artists_list' + str(datetime.now()) + '.csv'
        artists_buffer = StringIO()
        artist_df.to_csv(artists_buffer, index=False)
        artists_content = artists_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=artist_key, Body=artists_content)
        
        tracks_key = 'Transformed_Data/Tracks_Data/tracks_list' + str(datetime.now()) + '.csv'
        tracks_buffer = StringIO()
        track_df.to_csv(tracks_buffer, index=False)
        tracks_content = tracks_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=tracks_key, Body=tracks_content)
        
    # Copy the original data files from 'Before_Processed_Data' to 'After_Processed_Data' and delete the original.
    s3_resource = boto3.resource('s3')
    for key in spotify_file_names:
        copy_source = {
            'Bucket' : bucket,
            'Key' : key
        }
        s3_resource.meta.client.copy(copy_source, bucket, 'Raw_Data/After_Processed_Data/' + key.split('/')[-1])
        s3_resource.Object(bucket, key).delete()