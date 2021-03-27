```python
import sys
sys.path.insert(0, './CPPotify/source/py')
from CPPotify import CPPotify

from keys import (USER, PASS, ACCOUNT, CLIENT_ID, CLIENT_SECRET,
                  REDIRECT_URI, STATE, SCOPE, SHOW_DIALOG,
                  ID, SECRET)

import snowflake.connector
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

bucket = 'spotifyrecsystem'

cpp = CPPotify(CLIENT_ID, CLIENT_SECRET)
```

## Old Snowflake code - migrated to AWS


```python
def checkpoint():
    ctx = snowflake.connector.connect(
        user = USER,
        password = PASS,
        account = ACCOUNT,print(e)
        warehouse = 'COMPUTE_WH',
        database = 'SPOTIFY_DB'
    )
    cs = ctx.cursor()

    if len(playlist_id_list) != 0:client('connect')
        if len(playlist_id_list) >= 16384:
            for x in range(math.ceil(len(playlist_id_list)/16384)):
                temp = playlist_id_list[x * 16384 : (x * 16384) + 16384]
                playlist_id_tuple = [[str(x)] for x in temp]
                try:
                    query_string = "INSERT INTO PUBLIC.PLAYLIST_IDS(ID) VALUES (%s)"
                    cs.executemany(query_string, playlist_id_tuple)
                except Exception as e:
                    print(e)
        else:
            playlist_id_tuple = [[str(x)] for x in playlist_id_list]
            try:
                query_string = "INSERT INTO PUBLIC.PLAYLIST_IDS(ID) VALUES (%s)"
                cs.executemany(query_string, playlist_id_tuple)
            except Exception as e:
                print(e)

    if len(album_id_list) != 0:
        if len(album_id_list) >= 16384:
            for x in range(math.ceil(len(album_id_list)/16384)):
                temp = album_id_list[x * 16384 : (x * 16384) + 16384]
                album_id_tuple = [[str(x)] for x in temp]
                try:
                    query_string = "INSERT INTO PUBLIC.ALBUM_IDS(ID) VALUES (%s)"
                    cs.executemany(query_string, album_id_tuple)
                except Exception as e:
                    print(e)
        else:
            album_id_tuple = [[str(x)] for x in album_id_list]
            try:
                query_string = "INSERT INTO PUBLIC.ALBUM_IDS(ID) VALUES (%s);"
                cs.executemany(query_string, album_id_tuple)
            except Exception as e:
                print(e)

    if len(artist_id_list) != 0:
        if len(artist_id_list) >= 16384:
            for x in range(math.ceil(len(artist_id_list)/16384)):
                temp = artist_id_list[x * 16384 : (x * 16384) + 16384]
                artist_id_tuple = [[str(x)] for x in temp]
                try:
                    query_string = "INSERT INTO PUBLIC.ARTIST_IDS(ID) VALUES (%s)"
                    cs.executemany(query_string, artist_id_tuple)
                except Exception as e:
                    print(e)
        else:
            artist_id_tuple = [[str(x)] for x in artist_id_list]
            try:
                query_string = "INSERT INTO PUBLIC.ARTIST_IDS(ID) VALUES (%s);"
                cs.executemany(query_string, artist_id_tuple)
            except Exception as e:
                print(e)

    if len(track_id_list) != 0:
        if len(track_id_list) >= 16384:
            for x in range(math.ceil(len(track_id_list)/16384)):
                temp = track_id_list[x * 16384 : (x * 16384) + 16384]
                track_id_tuple = [[str(x)] for x in temp]
                try:
                    query_string = "INSERT INTO PUBLIC.TRACK_IDS(ID) VALUES (%s)"
                    cs.executemany(query_string, track_id_tuple)
                except Exception as e:
                    print(e)
        else:
            track_id_tuple = [[str(x)] for x in track_id_list]
            try:
                query_string = "INSERT INTO PUBLIC.TRACK_IDS(ID) VALUES (%s);"
                cs.executemany(query_string, track_id_tuple)
            except Exception as e:
                print(e)

    ctx.close()
    cs.close()
```


```python
def audio_features_only_checkpoint():
    ctx = snowflake.connector.connect(
        user = USER,
        password = PASS,
        account = ACCOUNT,
        warehouse = 'COMPUTE_WH',
        database = 'SPOTIFY_DB'
    )
    cs = ctx.cursor()

    if len(audio_features_df) != 0:
        start = (len(audio_features_df)//10000 - 1) * 10000
        audio_features_tuple = list(audio_features_df.iloc[start : start + 10000].itertuples(index=False, name=None))
        try:
            query_string = "INSERT INTO PUBLIC.AUDIO_FEATURES_ONLY(DANCEABILITY, ENERGY, KEY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS, INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO, TYPE, ID, URI, TRACK_HREF, ANALYSIS_URL, DURATION_MS, TIME_SIGNATURE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cs.executemany(query_string, audio_features_tuple)
        except Exception as e:
            print(e)
            
    ctx.close()
    cs.close()
```


```python
def audio_features_full_checkpoint():
    ctx = snowflake.connector.connect(
        user = USER,
        password = PASS,
        account = ACCOUNT,
        warehouse = 'COMPUTE_WH',
        database = 'SPOTIFY_DB'
    )
    cs = ctx.cursor()

    if len(audio_features_full_df) != 0:
        start = (len(audio_features_full_df)//10000 - 1) * 10000
        audio_features_tuple = audio_features_full_df.iloc[start : start + 10000].values.tolist()
        try:
            query_string = "INSERT INTO PUBLIC.AUDIO_FEATURES_FULL(DANCEABILITY, ENERGY, KEY, LOUDNESS, MODE, SPEECHINESS, ACOUSTICNESS, INSTRUMENTALNESS, LIVENESS, VALENCE, TEMPO, TYPE, ID, URI, TRACK_HREF, ANALYSIS_URL, DURATION_MS, TIME_SIGNATURE, NAME, ARTIST_NAME, RELEASE_DATE) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            cs.executemany(query_string, audio_features_tuple)
        except Exception as e:
            print(e)
            
    ctx.close()
    cs.close()
```

## AWS

### Functions to save scraped Spotify data to S3


```python
def checkpoint(*args):
    client = boto3.client(
        's3',
        aws_access_key_id = ID, 
        aws_secret_access_key = SECRET,
        region_name = 'us-west-1'
    )
    
    resource = boto3.resource(
        's3',
        aws_access_key_id = ID, 
        aws_secret_access_key = SECRET,
        region_name = 'us-west-1'
    )

    for arg in args:
        try:
            if len(arg) > 0:
                csv_buffer = StringIO()
                spotify_obj = arg[0][0]
                
                if spotify_obj == 'albums':
                    df = pd.DataFrame([x[1:] for x in arg], columns = ['ind', 'id', 'artist_id']).to_csv(csv_buffer, index=False)

                elif spotify_obj == 'tracks':
                    df = pd.DataFrame([x[1:] for x in arg], columns = ['ind', 'id', 'artist_id', 'album_id', 'release_date', 'name']).to_csv(csv_buffer, index=False)

                elif spotify_obj == 'artists':
                    df = pd.DataFrame([x[1:] for x in arg], columns = ['id']).to_csv(csv_buffer, index=False)

                elif spotify_obj == 'playlists':
                    df = pd.DataFrame([x[1:] for x in arg], columns = ['id']).to_csv(csv_buffer, index=False)
                
                elif spotify_obj == 'audio_features':
                    df = pd.DataFrame([x[1:] for x in arg], columns = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'type', 'id', 'uri', 'track_href', 'analysis_url', 'duration_ms', 'time_signature']).to_csv(csv_buffer, index=False)
                
                old_obj = [obj['Key'] for obj in client.list_objects(Bucket='spotifyrecsystem')['Contents'] if obj['Key'] == spotify_obj + '/' + spotify_obj + '_data_cache.csv']
                if len(old_obj) > 0:
                    resource.Object('spotifyrecsystem', old_obj[0]).delete()

                resource.Object(bucket, spotify_obj + '/' + spotify_obj + '_data_cache.csv').put(Body=csv_buffer.getvalue())
        except Exception as e:
            raise Exception(e)
```


```python
def reload_lists():
    client = boto3.client(
        's3',
        aws_access_key_id = ID, 
        aws_secret_access_key = SECRET,
        region_name = 'us-west-1'
    )

    resource = boto3.resource(
        's3',
        aws_access_key_id = ID, 
        aws_secret_access_key = SECRET,
        region_name = 'us-west-1'
    )
    
    for obj in ['albums', 'artists', 'tracks', 'playlists']:
        obj_key = [objects['Key'] for objects in client.list_objects(Bucket='spotifyrecsystem')['Contents'] if objects['Key'] == obj + '/' + obj + '_data_cache.csv']
        resource.Bucket(bucket).download_file(obj_key[0], 'reload/'+obj+'_data_cache.csv')
```


```python
playlist_id_list = []
album_id_list = []
track_id_list = []
artist_id_list = []
```


```python
for item in cpp.browse('categories')['categories']['items']:
    playlists = cpp.browse('categories', item['id'], 'playlists')
    try:
        for playlist in playlists['playlists']['items']:
            if playlist['id'] not in [x[1] for x in playlist_id_list]:
                playlist_id_list.append(('playlists', playlist['id']))
    except Exception as e:
        print(e)
```


```python
checkpoint(playlist_id_list)
```


```python
ind = 1
for album in cpp.browse('new-releases')['albums']['items']:
    for artist in album['artists']:
        if artist not in [x[1] for x in artist_id_list]:
            artist_id_list.append(('artists', artist['id']))
    if len(album['artists']) > 1:
        for art in range(len(album['artists'])):
            album_id_list.append(('albums', ind, album['id'], album['artists'][art]['id']))
            ind += 1
    else:
        if album['id'] not in [x[2] for x in album_id_list]: 
            album_id_list.append(('albums', ind, album['id'], album['artists'][0]['id']))
            ind += 1
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list)
```


```python
for y in range(2013, 2021):
    for m in range(1, 13):
        for d in range(1, 32):
            try:
                featured = cpp.browse('featured-playlists', timestamp=datetime(y, m, d, 8))['playlists']['items']
                for playlist in featured:
                    if playlist['id'] not in [x[1] for x in playlist_id_list]: 
                        playlist_id_list.append(('playlists', playlist['id']))
            except Exception as e:
                print(e)
                pass
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list)
```


```python
track_ind = 0
ind = album_id_list[-1][1] + 1
for playlist_id in playlist_id_list:
    try:
        res = cpp.get_playlists(False, '', playlist_id[1], 'tracks', limit=100)['items']
        for track in res:
            if track['track']['album']['id'] not in [x[2] for x in album_id_list]: 
                if len(track['track']['artists']) > 1:
                    for art in range(len(track['track']['artists'])):
                        album_id_list.append(('albums', ind, track['track']['album']['id'], track['track']['artists'][art]['id']))
                        ind += 1
                else:
                    album_id_list.append(('albums', ind, track['track']['album']['id'], track['track']['artists'][0]['id']))
                    ind += 1
            for artist in track['track']['artists']:
                if artist['id'] not in [x[1] for x in artist_id_list]: 
                    artist_id_list.append(('artists', artist['id']))
            if track['track']['id'] not in [x[2] for x in track_id_list]:
                artist_albums = []
                if len(track['track']['artists']) > 1:
                     for art in range(len(track['track']['artists'])):
                        track_id_list.append(('tracks', track_ind, track['track']['id'], track['track']['artists'][art]['id'], track['track']['album']['id'], track['track']['album']['release_date'], track['track']['name']))
                        track_ind += 1
                else:
                    track_id_list.append(('tracks', track_ind, track['track']['id'], track['track']['artists'][0]['id'], track['track']['album']['id'], track['track']['album']['release_date'], track['track']['name']))
                    track_ind += 1
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
```


```python
for id in artist_id_list:
    try:
        res = cpp.get_artists(id[1], 'related-artists')['artists']
        for rel_artist in res:
            if rel_artist['id'] not in [x[1] for x in artist_id_list]:
                 artist_id_list.append(('artists', rel_artist['id']))
        if len(artist_id_list) % 10000 == 0:
            print(len(artist_id_list))
            checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
```


```python
ind = album_id_list[-1][1] + 1
for id in artist_id_list:
    try:
        res = cpp.get_artists(id[1], 'albums', include_groups='album,single,appears_on,compilation')
        for album in res['items']:
            if album['id'] not in [x[2] for x in album_id_list]:
                if len(album['artists']) > 1:
                    for art in range(len(album['artists'])):
                        album_id_list.append(('albums', ind, album['id'], album['artists'][art]['id']))
                        ind += 1
                else:
                    album_id_list.append(('albums', ind, album['id'], id[1]))
                    ind += 1
        if len(album_id_list) % 10000 == 0:
            print(len(album_id_list))
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
```


```python
ind = track_id_list[-1][1] + 1
for id in list(set([x[2] for x in album_id_list if x not in [x[4] for x in track_id_list]])):
    try:
        res = cpp.get_albums(id)
        for tracks in res['tracks']['items']:
            if tracks['id'] not in [x[2] for x in track_id_list]:
                if len(tracks['artists']) > 1:
                    for art in range(len(tracks['artists'])):
                        track_id_list.append(('tracks', ind, tracks['id'], tracks['artists'][art]['id'], id, res['release_date'], tracks['name']))
                        ind += 1
                else:
                    track_id_list.append(('tracks', ind, tracks['id'], tracks['artists'][0]['id'], id, res['release_date'], tracks['name']))
                    ind += 1
        if len(track_id_list) % 1000 == 0:
            print(len(track_id_list))
            checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```


```python
checkpoint(playlist_id_list, artist_id_list, album_id_list, track_id_list)
```


```python
features = []
```


```python
for interval in range(math.ceil(len(set([x[2] for x in track_id_list])) / 50)):
    try:
        for dic in [x for x in cpp.get_tracks([x[2] for x in track_id_list][interval * 50 : (interval * 50) + 50], 'audio-features')['audio_features'] if x is not None]:
            features.append(tuple(['audio_features'] + list(dic.values())))
            if len(features) % 10000 == 0:
                print(len(features))
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```


```python
checkpoint(features)
```


```python
audio_features_full_df = audio_features_df

for row in range(len(audio_features_df)):
    try:
        track = cpp.get_tracks(audio_features_df.iloc[row]['id'])

        audio_features_full_df.at[row, 'name'] = track['name']
        audio_features_full_df.at[row, 'artist_name'] = track['artists'][0]['name']
        audio_features_full_df.at[row, 'release_date'] = track['album']['release_date']
        
        if len(audio_features_full_df) % 10000 == 0:
            audio_features_full_checkpoint()
            print(len(audio_features_full_df))
    except Exception as e:
        print('Exception occured: ' + str(e))
        pass
```
