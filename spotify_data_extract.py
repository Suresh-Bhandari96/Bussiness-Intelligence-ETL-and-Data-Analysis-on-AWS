# This Lambda function interacts with the Spotify Web API to retrieve track data from a specific playlist,
# then stores this data as a JSON object in an Amazon S3 bucket. It is triggered by an event and executed in AWS Lambda.

import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    playlist_link = "https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF"
    playlist_URI = playlist_link.split("/")[4]
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    client = boto3.client('s3')
    # Generate a unique filename for the JSON data based on the current timestamp.
    filename = 'spotify_rawdata' + str(datetime.now()) + '.json'
    # Upload the Spotify data as a JSON object to an S3 bucket with the specified key.
    client.put_object(
        Bucket = 'spotify-etl-pipeline-96',
        Key = 'Raw_Data/Before_Processed_data/' + filename,
        Body = json.dumps(spotify_data)
        )