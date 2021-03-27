```python
import sys
import json
sys.path.insert(0, './CPPotify/source/py')
from CPPotify import CPPotify

import numpy as np
import pandas as pd

from utils.utils import cosine_similarity, jaccard_similarity, list_union
from keys import CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, STATE, SCOPE, SHOW_DIALOG

from playlist_str import playlist_str as get_playlists_results
```


```python
pd.options.display.precision = 10
```


```python
cpp = CPPotify(CLIENT_ID, CLIENT_SECRET, REDIRECT_URI, STATE, SCOPE, SHOW_DIALOG)
```


```python
cpp.open_auth_url()
```


```python
cpp.oAuth_flow('https://example.com/callback?code=AQDdGsGF40NWkj-1s28xzpi8D2InCvjsorCGpxH0PnjhYNG_2o87Ga1EqP-tDVuXT6jIAe3RpAZMXyNlq_JYL34AIgf-iH7oFIAUPaelhFrJB48a5N9_Mg0eKJysyRxoc2WwAyuBFvwsGyKmEwX0F7Mn9zcMeYbCxDgMK2_xohEiXaqm_X7g0exXNifV6Eh8vYxP2odFQYHe7USjdPvukksUs1BZ43PXE8riYwI4VIcvSzQY9gfDUlLeiqKWnUgYnjzDTNxF66MXXi2u8u3gcsRjLnJTwjD2M3A44OlXbfBLJ2RDhSEzYf5G6z9mQe--DGiX2qyc8eqUMMU-2ww_cSCDFIiehkO7I99KTpxWkw&state=34fFs29kd09')
```


```python
dat = pd.read_csv('../audio_features_full/features_full.csv')
```


```python
dat
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>track_id</th>
      <th>name</th>
      <th>album_id</th>
      <th>artist_id</th>
      <th>release_date</th>
      <th>danceability</th>
      <th>energy</th>
      <th>key</th>
      <th>loudness</th>
      <th>mode</th>
      <th>...</th>
      <th>liveness</th>
      <th>valence</th>
      <th>tempo</th>
      <th>type</th>
      <th>id</th>
      <th>uri</th>
      <th>track_href</th>
      <th>analysis_url</th>
      <th>duration_ms</th>
      <th>time_signature</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>6Nd6ntkzr4t8o1FKPGOSMt</td>
      <td>Here I Go Again - 2018 Remaster</td>
      <td>70uPhkIhXTXM6MNNsMjAHx</td>
      <td>3UbyYnvNIT5DFXU4WgiGpP</td>
      <td>NaN</td>
      <td>0.292</td>
      <td>0.823</td>
      <td>7</td>
      <td>-6.763</td>
      <td>1</td>
      <td>...</td>
      <td>0.1550</td>
      <td>0.244</td>
      <td>89.939</td>
      <td>audio_features</td>
      <td>6Nd6ntkzr4t8o1FKPGOSMt</td>
      <td>spotify:track:6Nd6ntkzr4t8o1FKPGOSMt</td>
      <td>https://api.spotify.com/v1/tracks/6Nd6ntkzr4t8...</td>
      <td>https://api.spotify.com/v1/audio-analysis/6Nd6...</td>
      <td>275395</td>
      <td>4</td>
    </tr>
    <tr>
      <th>1</th>
      <td>31DvHUCSioX0JD7B4kZMJ9</td>
      <td>Money For Nothing</td>
      <td>15J400U0rEpgE64UQgtvLs</td>
      <td>0WwSkZ7LtFUFjGjMZBMt6T</td>
      <td>1985-05-13</td>
      <td>0.666</td>
      <td>0.667</td>
      <td>2</td>
      <td>-9.305</td>
      <td>0</td>
      <td>...</td>
      <td>0.0740</td>
      <td>0.679</td>
      <td>134.219</td>
      <td>audio_features</td>
      <td>31DvHUCSioX0JD7B4kZMJ9</td>
      <td>spotify:track:31DvHUCSioX0JD7B4kZMJ9</td>
      <td>https://api.spotify.com/v1/tracks/31DvHUCSioX0...</td>
      <td>https://api.spotify.com/v1/audio-analysis/31Dv...</td>
      <td>510933</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>6yBbmHFTxihIDFAerzDMGi</td>
      <td>Hold On Tight</td>
      <td>4k1GJg2poyo6hwWLqJn9C2</td>
      <td>7jefIIksOi1EazgRTfW2Pk</td>
      <td>1981-08-01</td>
      <td>0.439</td>
      <td>0.794</td>
      <td>7</td>
      <td>-11.792</td>
      <td>1</td>
      <td>...</td>
      <td>0.4340</td>
      <td>0.748</td>
      <td>145.056</td>
      <td>audio_features</td>
      <td>6yBbmHFTxihIDFAerzDMGi</td>
      <td>spotify:track:6yBbmHFTxihIDFAerzDMGi</td>
      <td>https://api.spotify.com/v1/tracks/6yBbmHFTxihI...</td>
      <td>https://api.spotify.com/v1/audio-analysis/6yBb...</td>
      <td>186600</td>
      <td>4</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2E2ZVy2fxslpAUgbb4zu84</td>
      <td>Abracadabra</td>
      <td>0afS7TjKOoq8LzTx9CgOnu</td>
      <td>6QtGlUje9TIkLrgPZrESuk</td>
      <td>1982-01-01</td>
      <td>0.791</td>
      <td>0.535</td>
      <td>9</td>
      <td>-13.261</td>
      <td>0</td>
      <td>...</td>
      <td>0.1560</td>
      <td>0.963</td>
      <td>127.488</td>
      <td>audio_features</td>
      <td>2E2ZVy2fxslpAUgbb4zu84</td>
      <td>spotify:track:2E2ZVy2fxslpAUgbb4zu84</td>
      <td>https://api.spotify.com/v1/tracks/2E2ZVy2fxslp...</td>
      <td>https://api.spotify.com/v1/audio-analysis/2E2Z...</td>
      <td>308373</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>17lu4tymfnhmcIDlzBbtAb</td>
      <td>For Those About to Rock (We Salute You)</td>
      <td>7DUvURQ0wfA1kgG8j99frR</td>
      <td>711MCceyCBcFnzjGY4Q7Un</td>
      <td>1981-11-23</td>
      <td>0.370</td>
      <td>0.919</td>
      <td>4</td>
      <td>-5.721</td>
      <td>1</td>
      <td>...</td>
      <td>0.4900</td>
      <td>0.471</td>
      <td>134.021</td>
      <td>audio_features</td>
      <td>17lu4tymfnhmcIDlzBbtAb</td>
      <td>spotify:track:17lu4tymfnhmcIDlzBbtAb</td>
      <td>https://api.spotify.com/v1/tracks/17lu4tymfnhm...</td>
      <td>https://api.spotify.com/v1/audio-analysis/17lu...</td>
      <td>344240</td>
      <td>4</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>178001</th>
      <td>099HTqcj7CVUhhEX37HAVO</td>
      <td>"Die Zauberflöte</td>
      <td>0NGpsNzek1XII5Wpsjhsqa</td>
      <td>7LKNaKHqFS4Achy6WyTBGw</td>
      <td>1984-01-01</td>
      <td>0.423</td>
      <td>0.117</td>
      <td>7</td>
      <td>-23.944</td>
      <td>1</td>
      <td>...</td>
      <td>0.0781</td>
      <td>0.266</td>
      <td>85.430</td>
      <td>audio_features</td>
      <td>099HTqcj7CVUhhEX37HAVO</td>
      <td>spotify:track:099HTqcj7CVUhhEX37HAVO</td>
      <td>https://api.spotify.com/v1/tracks/099HTqcj7CVU...</td>
      <td>https://api.spotify.com/v1/audio-analysis/099H...</td>
      <td>484000</td>
      <td>4</td>
    </tr>
    <tr>
      <th>178002</th>
      <td>7yaC9Ex1Tr63bXQJKuQmzx</td>
      <td>Broken Love</td>
      <td>1vwARFHu4r2ES0V94XC871</td>
      <td>0OaBO8SytZzvzAO3NOWiv3</td>
      <td>2020-04-17</td>
      <td>0.576</td>
      <td>0.709</td>
      <td>8</td>
      <td>-8.128</td>
      <td>0</td>
      <td>...</td>
      <td>0.1840</td>
      <td>0.163</td>
      <td>119.995</td>
      <td>audio_features</td>
      <td>7yaC9Ex1Tr63bXQJKuQmzx</td>
      <td>spotify:track:7yaC9Ex1Tr63bXQJKuQmzx</td>
      <td>https://api.spotify.com/v1/tracks/7yaC9Ex1Tr63...</td>
      <td>https://api.spotify.com/v1/audio-analysis/7yaC...</td>
      <td>177255</td>
      <td>4</td>
    </tr>
    <tr>
      <th>178003</th>
      <td>6gD72JkhAT2zdX2GM5qzgR</td>
      <td>Mi Fai Stare Bene</td>
      <td>3m0EaT2PtiQNx9b5AHbtLF</td>
      <td>0Qd65xBSFzdm3zCEu2ThQF</td>
      <td>2021-02-05</td>
      <td>0.498</td>
      <td>0.839</td>
      <td>9</td>
      <td>-7.000</td>
      <td>1</td>
      <td>...</td>
      <td>0.2090</td>
      <td>0.701</td>
      <td>162.097</td>
      <td>audio_features</td>
      <td>6gD72JkhAT2zdX2GM5qzgR</td>
      <td>spotify:track:6gD72JkhAT2zdX2GM5qzgR</td>
      <td>https://api.spotify.com/v1/tracks/6gD72JkhAT2z...</td>
      <td>https://api.spotify.com/v1/audio-analysis/6gD7...</td>
      <td>224627</td>
      <td>4</td>
    </tr>
    <tr>
      <th>178004</th>
      <td>6uOM2nnMDyiWFqIIPKc155</td>
      <td>Barbie Girl</td>
      <td>3m0EaT2PtiQNx9b5AHbtLF</td>
      <td>6kBjAFKyd0he7LiA5GQ3Gz</td>
      <td>2021-02-05</td>
      <td>0.755</td>
      <td>0.952</td>
      <td>1</td>
      <td>-4.420</td>
      <td>0</td>
      <td>...</td>
      <td>0.4660</td>
      <td>0.929</td>
      <td>129.965</td>
      <td>audio_features</td>
      <td>6uOM2nnMDyiWFqIIPKc155</td>
      <td>spotify:track:6uOM2nnMDyiWFqIIPKc155</td>
      <td>https://api.spotify.com/v1/tracks/6uOM2nnMDyiW...</td>
      <td>https://api.spotify.com/v1/audio-analysis/6uOM...</td>
      <td>195040</td>
      <td>4</td>
    </tr>
    <tr>
      <th>178005</th>
      <td>7wzcyrNodEDUN1AFLcpKnS</td>
      <td>Glory Box</td>
      <td>3m0EaT2PtiQNx9b5AHbtLF</td>
      <td>6liAMWkVf5LH7YR9yfFy1Y</td>
      <td>2021-02-05</td>
      <td>0.510</td>
      <td>0.433</td>
      <td>1</td>
      <td>-10.006</td>
      <td>1</td>
      <td>...</td>
      <td>0.0603</td>
      <td>0.208</td>
      <td>119.695</td>
      <td>audio_features</td>
      <td>7wzcyrNodEDUN1AFLcpKnS</td>
      <td>spotify:track:7wzcyrNodEDUN1AFLcpKnS</td>
      <td>https://api.spotify.com/v1/tracks/7wzcyrNodEDU...</td>
      <td>https://api.spotify.com/v1/audio-analysis/7wzc...</td>
      <td>308627</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
<p>178006 rows × 23 columns</p>
</div>




```python
def get_song_artist_df(followed_playlists):
    # Take the playlists that a user follows. For each playlist in that list, create a dictionary having keys set to each unique track ID, with values set to 
    # the artists that are featured on that track. 
    
    followed_tracks_artists = {}
    for playlist_id in followed_playlists:
        try:
            res = cpp.get_playlists(False, '', playlist_id, 'tracks')
            for track in res['items']:
                if track['track']['id'] not in followed_tracks_artists.keys():
                    followed_tracks_artists[track['track']['id']] = [art['id'] for art in track['track']['artists']]
                else:
                    for artist in track['track']['artists']:
                        followed_tracks_artists[track['track']['id']].append(artist['id'])
        except Exception as e:
            print(e)
            pass

    # Create a DataFrame containing with each row containing a unique artist-track combinations 

    followed_tracks_df = pd.DataFrame(columns=['track', 'artist'])

    for track in followed_tracks_artists.keys():
        for row in followed_tracks_artists[track]:
            followed_tracks_df = followed_tracks_df.append({'track': track, 'artist': row}, ignore_index=True)
    
    # Now that we have a DataFrame of unique artist-track combinations, we want to find the genres for each unique artist. Spotify does not release track genre information
    # through its API, for this reason we have to find the genre of the artist. We will use this genre information to filter the final DataFrame to match genres with the song/artist
    # that is currently being listened to

    artist_genre = {}
    for artist_id in followed_tracks_df['artist'].unique():
        if artist_id not in artist_genre.keys():
            artist_genre[artist_id] = cpp.get_artists(artist_id)['genres'] if cpp.get_artists(artist_id)['genres'] != [] and 'genres' in cpp.get_artists(artist_id).keys() else None
    
    # For the newly created artist_genre dictionary above, match each artist ID in our previously created DataFrame with its genre in the dictionary. Use this to create a new 'genre'
    # column in the DataFrame. Filter out any artists that do not have genre information

    followed_tracks_df['genres'] = [artist_genre[id] for id in followed_tracks_df['artist']]
    followed_tracks_df = followed_tracks_df[followed_tracks_df['genres'].notna()]

    # For the newly created 'genre' column, split the genres into words and create a union of those words for each artist. The reason we do this is that Spotify contains a lot of unique 
    # genres. Since we have a rather small dataset, we want to split more rare genres such as 'german hip hop' into a set ('german', 'hip', 'hop') so that this song will match with other 
    # german or hip-hop tracks
    
    followed_tracks_df['genre_words'] = [list_union([word for word in [genre_string.split(' ') for genre_string in genre_list]]) for genre_list in followed_tracks_df['genres'].values]

    return followed_tracks_df
```


```python
def current_song_info(song_id)->tuple:
    # For the current song, get the song's artists and the genres for those artists

    track_info = cpp.get_tracks(song_id)
    
    current_artists = [artist['id'] for artist in track_info['artists']]
    current_genres = {}
    for artist in current_artists:
        try:
            current_genres[artist] = cpp.get_artists(artist)['genres']
        except Exception as e:
            current_genres[artist] = None
                
    return current_genres
```


```python
def get_audio_features(tracks, existing_data):
    # Create a dictionary of tracks and their audio features. If the song exists in the current dataset, retrieve that information. If it doesn't, retrieve the information from the 
    # Spotify API 

    audio_features = {}
    for id in tracks:
        if id in existing_data['track_id'].values:
            audio_features[id] = existing_data[existing_data['track_id']==id].iloc[:, np.r_[5:16, 21:23]].values.tolist()
        else:
            try:
                audio_features[id] = list({k:v for k,v in cpp.get_tracks(id, 'audio-features').items() if k in existing_data.columns[np.r_[5:16, 21:23]].values}.values())
            except:
                pass 
    
    return audio_features
```


```python
def song_rec(user_id, get_playlists_results, current_song, stored_songs)->str:
    # Given a user_id, find all playlists that the user follows. Using this logic, we can create a set of tracks listened to by users that created these followed playlists.
    # Since the Spotify API does not let us find followed users, this is our best bet for collaborative filtering

    followed_playlists = [playlist['id'] for playlist in json.loads(get_playlists_results)['items'] if playlist['owner']['id'] != user_id]
    
    # Call the two functions above to get information for tracks contained in 'followed playlists', as well as track information for the current song

    followed_tracks_df = get_song_artist_df(followed_playlists)
    current_genres = current_song_info(current_song)

    # For the genres of the current song/artist(s)
    
    current_genres_words = list_union([word for word in [dict_values.split(' ') for all_genres in current_genres.values() for dict_values in all_genres]])
    
    # Find the set Jaccard distance between the genres of the current song/artist(s) and all the songs in our followed tracks dataset. Since Spotify has unique genre naming conventions,
    # it will be harder to find exact matches for sets of genres 
    
    followed_tracks_df['jac_dist'] = [round(jaccard_similarity(np.array(current_genres_words), np.array(followed_genre)), 2) for followed_genre in followed_tracks_df['genre_words']]

    # Keep songs which genre sets have Jaccard distance greater than or equal to 0.4 
    
    followed_tracks_df = followed_tracks_df[followed_tracks_df['jac_dist'] >= 0.4] 

    # Get the audio features for the songs in the remaining dataset
    
    followed_tracks_audio_features = get_audio_features(followed_tracks_df['track'].unique(), stored_songs)

    # Set the audio_features retrieved above as a new column in the dataframe
    
    followed_tracks_df['audio_features'] = [followed_tracks_audio_features[track] if track in followed_tracks_audio_features else None for track in followed_tracks_df['track']]
    followed_tracks_df = followed_tracks_df[followed_tracks_df['audio_features'].notna()]

    # Get the audio_features of the current song. We will be calculating the cosine similarity of the audio features of the current song to each song in our dataframe
    
    af_list_values = list({k:v for k,v in cpp.get_tracks(current_song, 'audio-features').items() if k in stored_songs.columns[np.r_[5:16, 21:23]].values}.values())

    # Now that we have the audio features of the current song and the songs with closely matching genres, we can calculate cosine similarity. Sort the 
    # DataFrame by cosine similarity and Jaccard distance. Then return the DataFrame

    followed_tracks_df['cosine_sim'] = [cosine_similarity(np.array(af_list_values), stored_songs.iloc[x, np.r_[5:16, 21:23]].values) for x in range(len(followed_tracks_df))]
    followed_tracks_df.sort_values(['jac_dist', 'cosine_sim'], ascending=False)
   
    return followed_tracks_df
```


```python
followed_playlists = song_rec('1252723390', get_playlists_results, '0DAQryfUiFZdWFo76RYBc8', dat)
```


```python
test = get_audio_features(followed_playlists[followed_playlists['jac_dist']>=0.4]['track'].unique(), dat)
```


```python
followed_playlists
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>track</th>
      <th>artist</th>
      <th>genres</th>
      <th>genre_words</th>
      <th>jac_dist</th>
      <th>audio_features</th>
      <th>cosine_sim</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>39</th>
      <td>3qAyjdhtnU2a14rrRKEkcE</td>
      <td>02kJSzxNuaWGqwubyUba0Z</td>
      <td>[hip hop, indie pop rap, oakland hip hop, pop ...</td>
      <td>{hop, hip, oakland, rap, indie, pop}</td>
      <td>0.43</td>
      <td>[0.836, 0.703, 0, -4.732, 1, 0.107, 0.0301, 0,...</td>
      <td>0.9999999626</td>
    </tr>
    <tr>
      <th>40</th>
      <td>1cTZMwcBJT0Ka3UJPXOeeN</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.567, 0.913, 8, -6.471, 1, 0.0736, 0.0934, 0...</td>
      <td>0.9999999421</td>
    </tr>
    <tr>
      <th>43</th>
      <td>6n4U3TlzUGhdSFbUUhTvLP</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.907, 0.633, 2.0, -5.145, 1.0, 0.184, 0.087...</td>
      <td>0.9999999835</td>
    </tr>
    <tr>
      <th>47</th>
      <td>4qKcDkK6siZ7Jp1Jb4m0aL</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.922, 0.581, 10.0, -7.495, 1.0, 0.27, 0.001...</td>
      <td>0.9999999826</td>
    </tr>
    <tr>
      <th>56</th>
      <td>7sO5G9EABYOXQKNPNiE9NR</td>
      <td>0iEtIxbK0KxaSlF7G42ZOp</td>
      <td>[hip hop, pop rap, rap, southern hip hop, trap]</td>
      <td>{hop, hip, southern, rap, pop, trap}</td>
      <td>0.43</td>
      <td>[[0.88, 0.428, 9.0, -8.28, 1.0, 0.206, 0.149, ...</td>
      <td>0.9999999768</td>
    </tr>
    <tr>
      <th>58</th>
      <td>7sO5G9EABYOXQKNPNiE9NR</td>
      <td>0iEtIxbK0KxaSlF7G42ZOp</td>
      <td>[hip hop, pop rap, rap, southern hip hop, trap]</td>
      <td>{hop, hip, southern, rap, pop, trap}</td>
      <td>0.43</td>
      <td>[[0.88, 0.428, 9.0, -8.28, 1.0, 0.206, 0.149, ...</td>
      <td>0.9999999556</td>
    </tr>
    <tr>
      <th>62</th>
      <td>4XoP1AkbOurU9CeZ2rMEz2</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.869, 0.687, 1, -6.816, 1, 0.263, 0.0208, 1e...</td>
      <td>0.9999999910</td>
    </tr>
    <tr>
      <th>76</th>
      <td>2Xqd0wUttjueBfdcltADOv</td>
      <td>02kJSzxNuaWGqwubyUba0Z</td>
      <td>[hip hop, indie pop rap, oakland hip hop, pop ...</td>
      <td>{hop, hip, oakland, rap, indie, pop}</td>
      <td>0.43</td>
      <td>[0.838, 0.771, 1, -3.791, 1, 0.244, 0.0117, 0,...</td>
      <td>0.9999999994</td>
    </tr>
    <tr>
      <th>92</th>
      <td>6UjfByV1lDLW0SOVQA4NAi</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.877, 0.391, 1, -8.196, 1, 0.063, 0.0317, 0....</td>
      <td>0.9999999840</td>
    </tr>
    <tr>
      <th>94</th>
      <td>6fwdbPMwP1zVStm8FybmkO</td>
      <td>0iEtIxbK0KxaSlF7G42ZOp</td>
      <td>[hip hop, pop rap, rap, southern hip hop, trap]</td>
      <td>{hop, hip, southern, rap, pop, trap}</td>
      <td>0.43</td>
      <td>[0.835, 0.413, 1, -9.81, 1, 0.396, 0.373, 0.00...</td>
      <td>0.9999999885</td>
    </tr>
    <tr>
      <th>97</th>
      <td>39hnH8WdPmNT3Q3yzwC9Rg</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.926, 0.336, 0, -9.326, 0, 0.594, 0.0392, 0,...</td>
      <td>0.9999999619</td>
    </tr>
    <tr>
      <th>120</th>
      <td>17Q87zeXgsAi9iQQbMu9v0</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.594, 0.46, 1, -7.607, 1, 0.382, 0.00354, 2....</td>
      <td>0.9999999620</td>
    </tr>
    <tr>
      <th>121</th>
      <td>27GmP9AWRs744SzKcpJsTZ</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.852, 0.553, 1.0, -7.286, 1.0, 0.187, 0.055...</td>
      <td>0.9999999947</td>
    </tr>
    <tr>
      <th>123</th>
      <td>2Vx8E3K5icPZR7OCklWBXX</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[0.909, 0.405, 6, -8.133, 0, 0.14, 0.0306, 0.0...</td>
      <td>0.9999999476</td>
    </tr>
    <tr>
      <th>125</th>
      <td>794QVQtFNy7hvtjQP1keQd</td>
      <td>02kJSzxNuaWGqwubyUba0Z</td>
      <td>[hip hop, indie pop rap, oakland hip hop, pop ...</td>
      <td>{hop, hip, oakland, rap, indie, pop}</td>
      <td>0.43</td>
      <td>[0.817, 0.646, 4, -5.12, 0, 0.249, 0.0152, 0, ...</td>
      <td>0.9999999927</td>
    </tr>
    <tr>
      <th>153</th>
      <td>2IRZnDFmlqMuOrYOLnZZyc</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.889, 0.496, 4.0, -6.365, 0.0, 0.0905, 0.25...</td>
      <td>0.9999999215</td>
    </tr>
    <tr>
      <th>180</th>
      <td>4f8Mh5wuWHOsfXtzjrJB3t</td>
      <td>4V8LLVI7PbaPR0K2TGSxFF</td>
      <td>[hip hop, rap]</td>
      <td>{rap, hop, hip}</td>
      <td>0.40</td>
      <td>[[0.826, 0.579, 8.0, -8.241, 0.0, 0.0801, 0.00...</td>
      <td>0.9999999987</td>
    </tr>
    <tr>
      <th>193</th>
      <td>1auxYwYrFRqZP7t3s7w4um</td>
      <td>3nFkdlSjzX9mRTtwJOzDYB</td>
      <td>[east coast hip hop, hip hop, pop rap, rap]</td>
      <td>{hop, hip, coast, east, rap, pop}</td>
      <td>0.43</td>
      <td>[0.789, 0.858, 1, -5.542, 1, 0.311, 0.127, 0, ...</td>
      <td>0.9999999963</td>
    </tr>
    <tr>
      <th>206</th>
      <td>7Fa5UNizycSms5jP3SQD3F</td>
      <td>3nFkdlSjzX9mRTtwJOzDYB</td>
      <td>[east coast hip hop, hip hop, pop rap, rap]</td>
      <td>{hop, hip, coast, east, rap, pop}</td>
      <td>0.43</td>
      <td>[0.768, 0.807, 2, -5.508, 0, 0.106, 0.053, 2.9...</td>
      <td>0.9999999911</td>
    </tr>
    <tr>
      <th>209</th>
      <td>2DQ1ITjI0YoLFzuADN1ZBW</td>
      <td>02kJSzxNuaWGqwubyUba0Z</td>
      <td>[hip hop, indie pop rap, oakland hip hop, pop ...</td>
      <td>{hop, hip, oakland, rap, indie, pop}</td>
      <td>0.43</td>
      <td>[[0.838, 0.771, 1.0, -3.791, 1.0, 0.244, 0.011...</td>
      <td>0.9999999667</td>
    </tr>
    <tr>
      <th>215</th>
      <td>5WvAo7DNuPRmk4APhdPzi8</td>
      <td>1anyVhU62p31KFi8MEzkbf</td>
      <td>[chicago rap, conscious hip hop, hip hop, pop ...</td>
      <td>{hop, rap, chicago, hip, pop, conscious}</td>
      <td>0.43</td>
      <td>[0.552, 0.76, 0, -4.706, 1, 0.342, 0.0733, 0, ...</td>
      <td>0.9999999719</td>
    </tr>
    <tr>
      <th>221</th>
      <td>6wWaVoUOzLQJHd3bWAUpdZ</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.753, 0.686, 1.0, -5.415, 0.0, 0.284, 0.067...</td>
      <td>0.9999999669</td>
    </tr>
    <tr>
      <th>229</th>
      <td>7AijU6oTPGmG64uWf63Qvc</td>
      <td>68DWke2VjdDmA75aJX5C57</td>
      <td>[alabama rap, hip hop, pop rap, rap, southern ...</td>
      <td>{hop, hip, southern, rap, pop, alabama}</td>
      <td>0.43</td>
      <td>[0.622, 0.872, 2, -3.403, 1, 0.332, 0.349, 0, ...</td>
      <td>0.9999999907</td>
    </tr>
    <tr>
      <th>351</th>
      <td>65OVbaJR5O1RmwOQx0875b</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.578, 0.449, 1.0, -6.349, 1.0, 0.286, 0.061...</td>
      <td>0.9999999998</td>
    </tr>
    <tr>
      <th>375</th>
      <td>4FRW5Nza1Ym91BGV4nFWXI</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.77, 0.637, 1.0, -5.53, 1.0, 0.345, 0.103, ...</td>
      <td>0.9999999874</td>
    </tr>
    <tr>
      <th>377</th>
      <td>4FRW5Nza1Ym91BGV4nFWXI</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.77, 0.637, 1.0, -5.53, 1.0, 0.345, 0.103, ...</td>
      <td>0.9999999583</td>
    </tr>
    <tr>
      <th>383</th>
      <td>7eX3um6NpOQKWJMGCi97XD</td>
      <td>4FlG0V0jhLO4qGpayFOphj</td>
      <td>[kentucky hip hop]</td>
      <td>{hop, hip, kentucky}</td>
      <td>0.40</td>
      <td>[[0.779, 0.705, 6.0, -5.891, 0.0, 0.163, 0.004...</td>
      <td>0.9999999850</td>
    </tr>
    <tr>
      <th>389</th>
      <td>3aQem4jVGdhtg116TmJnHz</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.781, 0.594, 0.0, -6.959, 0.0, 0.0485, 0.01...</td>
      <td>0.9999999316</td>
    </tr>
    <tr>
      <th>396</th>
      <td>5SWnsxjhdcEDc7LJjq9UHk</td>
      <td>0iEtIxbK0KxaSlF7G42ZOp</td>
      <td>[hip hop, pop rap, rap, southern hip hop, trap]</td>
      <td>{hop, hip, southern, rap, pop, trap}</td>
      <td>0.43</td>
      <td>[[0.819, 0.626, 10.0, -4.574, 0.0, 0.202, 0.00...</td>
      <td>0.9999999529</td>
    </tr>
    <tr>
      <th>429</th>
      <td>7E2C5rBLpCKwQlhJPVFBRS</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.575, 0.609, 5.0, -4.88, 0.0, 0.121, 0.317,...</td>
      <td>0.9999999754</td>
    </tr>
    <tr>
      <th>434</th>
      <td>3Kwdm0iLyEiRB8InsZoo6n</td>
      <td>3TVXtAsR1Inumwj472S9r4</td>
      <td>[canadian hip hop, canadian pop, hip hop, pop ...</td>
      <td>{hop, canadian, hip, rap, pop, toronto}</td>
      <td>0.43</td>
      <td>[[0.773, 0.281, 1.0, -9.781, 1.0, 0.11, 0.24, ...</td>
      <td>0.9999999967</td>
    </tr>
    <tr>
      <th>444</th>
      <td>7r1lqyEW3iyZ39TDFnzI4p</td>
      <td>2mQLwfvZtvtTbipKn3xHmK</td>
      <td>[underground hip hop]</td>
      <td>{hop, hip, underground}</td>
      <td>0.40</td>
      <td>[0.76, 0.796, 11, -4.036, 0, 0.327, 0.384, 0, ...</td>
      <td>0.9999999715</td>
    </tr>
    <tr>
      <th>445</th>
      <td>7r1lqyEW3iyZ39TDFnzI4p</td>
      <td>2AfU5LYBVCiCtuCCfM7uVX</td>
      <td>[hip hop, pop rap, rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[0.76, 0.796, 11, -4.036, 0, 0.327, 0.384, 0, ...</td>
      <td>0.9999999930</td>
    </tr>
    <tr>
      <th>446</th>
      <td>2Wp2IjSLARq1XaoIMaDgCp</td>
      <td>2848adRcxvgWNRcz1g1tQD</td>
      <td>[pop rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[0.697, 0.53, 2, -8.15, 1, 0.313, 0.00831, 0, ...</td>
      <td>0.9999999945</td>
    </tr>
    <tr>
      <th>447</th>
      <td>2Wp2IjSLARq1XaoIMaDgCp</td>
      <td>2AfU5LYBVCiCtuCCfM7uVX</td>
      <td>[hip hop, pop rap, rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[0.697, 0.53, 2, -8.15, 1, 0.313, 0.00831, 0, ...</td>
      <td>0.9999999391</td>
    </tr>
    <tr>
      <th>449</th>
      <td>0DwMf9EDIXnFRTcZrePq6x</td>
      <td>2mQLwfvZtvtTbipKn3xHmK</td>
      <td>[underground hip hop]</td>
      <td>{hop, hip, underground}</td>
      <td>0.40</td>
      <td>[0.678, 0.627, 10, -10.659, 0, 0.164, 0.21, 0,...</td>
      <td>0.9999999829</td>
    </tr>
    <tr>
      <th>450</th>
      <td>49nkBg3OJtp6m6TQPmG1Qw</td>
      <td>2AfU5LYBVCiCtuCCfM7uVX</td>
      <td>[hip hop, pop rap, rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[0.613, 0.686, 1, -5.75, 1, 0.107, 0.0952, 0, ...</td>
      <td>0.9999999680</td>
    </tr>
    <tr>
      <th>467</th>
      <td>2nKaHCeL3e5DNl3P7eZ5uo</td>
      <td>1anyVhU62p31KFi8MEzkbf</td>
      <td>[chicago rap, conscious hip hop, hip hop, pop ...</td>
      <td>{hop, rap, chicago, hip, pop, conscious}</td>
      <td>0.43</td>
      <td>[[0.664, 0.66, 0.0, -5.284, 1.0, 0.291, 0.181,...</td>
      <td>0.9999999611</td>
    </tr>
    <tr>
      <th>473</th>
      <td>7ypuC93uVLeCbU58TtKQg9</td>
      <td>2AfU5LYBVCiCtuCCfM7uVX</td>
      <td>[hip hop, pop rap, rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[0.771, 0.459, 11, -13.681, 1, 0.234, 0.396, 0...</td>
      <td>0.9999999995</td>
    </tr>
    <tr>
      <th>476</th>
      <td>1jNKhbKmDgYSHHSCEeMXey</td>
      <td>2AfU5LYBVCiCtuCCfM7uVX</td>
      <td>[hip hop, pop rap, rap, underground hip hop]</td>
      <td>{hop, rap, underground, pop, hip}</td>
      <td>0.50</td>
      <td>[[0.637, 0.489, 2.0, -9.157, 1.0, 0.444, 0.136...</td>
      <td>0.9999999079</td>
    </tr>
    <tr>
      <th>483</th>
      <td>1tB6CvnAuQXn1hqa8ZsSDY</td>
      <td>2sVOqgur6zP6kXQ4EIv5R0</td>
      <td>[indie hip hop]</td>
      <td>{hop, indie, hip}</td>
      <td>0.40</td>
      <td>[0.817, 0.572, 1, -6.11, 1, 0.3, 0.12, 5.65e-0...</td>
      <td>0.9999999648</td>
    </tr>
    <tr>
      <th>488</th>
      <td>3lKfWEjux1xF8aKTOR5JQh</td>
      <td>3fhz0PcZrd3o3xB8APuoPX</td>
      <td>[portland hip hop]</td>
      <td>{hop, portland, hip}</td>
      <td>0.40</td>
      <td>[0.68, 0.523, 4, -5.619, 0, 0.262, 0.148, 0, 0...</td>
      <td>0.9999999733</td>
    </tr>
    <tr>
      <th>496</th>
      <td>7qW6buSnMfNRUkVCDJVge5</td>
      <td>2N2l2BR0KlnQrFnOn7ntEN</td>
      <td>[experimental hip hop]</td>
      <td>{hop, hip, experimental}</td>
      <td>0.40</td>
      <td>[0.651, 0.536, 2, -9.922, 1, 0.331, 0.245, 1.1...</td>
      <td>0.9999999477</td>
    </tr>
    <tr>
      <th>505</th>
      <td>67xMYvb2jcgUuxiak7SIPL</td>
      <td>2mQLwfvZtvtTbipKn3xHmK</td>
      <td>[underground hip hop]</td>
      <td>{hop, hip, underground}</td>
      <td>0.40</td>
      <td>[0.674, 0.61, 8, -8.156, 0, 0.45, 0.12, 0.0026...</td>
      <td>0.9999999956</td>
    </tr>
    <tr>
      <th>512</th>
      <td>2HrMDMckfCyM4OcZxXuiRn</td>
      <td>2tIP7SsRs7vjIcLrU85W8J</td>
      <td>[australian hip hop]</td>
      <td>{hop, hip, australian}</td>
      <td>0.40</td>
      <td>[0.412, 0.657, 6, -5.535, 0, 0.124, 0.468, 0, ...</td>
      <td>0.9999999773</td>
    </tr>
    <tr>
      <th>518</th>
      <td>2VIbFXPOrA6dDpCOT7iWvo</td>
      <td>5ZK5YGazICLXEXlaNnEBqP</td>
      <td>[indie hip hop]</td>
      <td>{hop, indie, hip}</td>
      <td>0.40</td>
      <td>[[0.579, 0.734, 8.0, -4.644, 1.0, 0.303, 0.321...</td>
      <td>0.9999999966</td>
    </tr>
  </tbody>
</table>
</div>




```python

```
