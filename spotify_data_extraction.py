import spotipy
from spotipy.oauth2 import SpotifyOAuth
import json
import boto3
from datetime import datetime
from dotenv import load_dotenv
import os 
from botocore.exceptions import NoCredentialsError
from prefect import flow, task

load_dotenv()

clientid = os.getenv('CLIENTID')
clientesecret = os.getenv('CLIENTSECRET')
FILE_NAME = 'spotify_data.json'


@task
def colecting_info(results):
    tracks = []
    for item in results['items']:
        track = {
            'name': item['name'],
            'artist': item['artists'][0]['name'],
            'album': item['album']['name'],
            'uri': item['uri']
        }
        tracks.append(track)
    return tracks
    
@task 
def save_file(tracks):
    with open(FILE_NAME, 'w') as f:
        json.dump(tracks, f)
    return FILE_NAME

    
# upload files to s3
@task
def upload_to_s3(file_name, bucket, region, object_name=None):
    if object_name is None:
        object_name = file_name

    s3_client = boto3.client('s3', region_name=region)

    try:
        s3_client.upload_file(file_name, bucket, object_name)
        print(f"Archivo {file_name} subido a {bucket}/{object_name}")
        return True
    except FileNotFoundError:
        print(f"El archivo {file_name} no fue encontrado")
        return False
    except NoCredentialsError:
        print("Credenciales no disponibles")
        return False

@flow
def spotify_to_s3(): 

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=clientid,
        client_secret=clientesecret,
        redirect_uri='http://example.com',
        scope='user-top-read'))

    #top listened songs
    results = sp.current_user_top_tracks(limit=50)

    tracks = colecting_info(results)
    save_file(tracks)
    
    bucket_name = 'spotify-bucket-test'
    region_name = 'sa-east-1'  # Cambia esto a la región donde está tu bucket
    upload_to_s3(FILE_NAME, bucket_name, region_name)
    
   
if __name__ == "__main__":
    spotify_to_s3()
    



