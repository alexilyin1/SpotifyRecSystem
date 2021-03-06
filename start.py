import sys
sys.path.insert(0, './CPPotify/source/py')
from CPPotify import CPPotify

from keys import (USER, PASS, ACCOUNT, CLIENT_ID, CLIENT_SECRET,
                  REDIRECT_URI, STATE, SCOPE, SHOW_DIALOG)
import snowflake.connector
import pandas as pd

ctx = snowflake.connector.connect(
    user = USER,
    password = PASS,
    account = ACCOUNT,
    warehouse = 'COMPUTE_WH',
    database = 'SPOTIFY_DB'
)

cpp = CPPotify(CLIENT_ID, CLIENT_SECRET)

playlist_id_list = []
album_id_list = []
artist_id_list = []

def to_db(playlist_id_list, album_id_list, artist_id_list):
    cs = ctx.cursor()

    if len(playlist_id_list) != 0:
        var_string = ", ".join('?' * len(playlist_id_list))        
        try:
            cs.execute("INSERT INTO PUBLIC.PLAYLIST_IDS(ID) VALUES (%s);", ','.join(list(map, str(playlist_id_list))))
        except Exception as e:
            print(e)
    
    if len(album_id_list) != 0:
        album_id_list = pd.DataFrame(album_id_list)
        try:
            cs.execute("INSERT INTO PUBLIC.ALBUM_IDS(ID) VALUES (%s)", ','.join(list(map(str, album_id_list))))
        except Exception as e:
            print(e)
    
    if len(artist_id_list) != 0:
        artist_id_list = pd.DataFrame(artist_id_list)
        try:
            cs.execute("INSERT INTO PUBLIC.ARTIST_IDS(ID) VALUES (%s)", ','.join(list(map(str, artist_id_list))))
        except Exception as e:
            print(e)

    cs.close()
        
def get_browse():
    '''for item in cpp.browse('categories')['categories']['items']:
        playlists = cpp.browse('categories', item['id'], 'playlists')
        try:
            for playlist in playlists['playlists']['items']:
                if playlist['id'] not in playlist_id_list:
                    playlist_id_list.append((playlist['id']))
        except:
            pass'''

    for album in cpp.browse('new-releases')['albums']['items']:
        for artist in album['artists']:
            if artist not in artist_id_list: artist_id_list.append(artist['id'])
        if album['id'] not in album_id_list: album_id_list.append(album['id'])
    '''
    for y in range(2013, 2021):
        for m in range(1, 13):
            for d in range(1, 32):
                try:
                    featured = cpp.browse('featured-playlists', timestamp=datetime(y, m, d, 8))['playlists']['items']
                    for playlist in featured:
                        if playlist['id'] not in playlist_id_list: playlist_id_list.append((playlist['id']))
                except Exception as e:
                    print(e)
                    pass'''

get_browse()
print(','.join('?' * len(album_id_list)))
to_db(playlist_id_list, album_id_list, artist_id_list)

ctx.close()
cs.close()