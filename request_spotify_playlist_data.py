import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
from dateutil import tz


def exportPlaylists(playlist_file):
    
    sp = spotipy.Spotify(auth_manager = SpotifyOAuth(
                         scope='playlist-read-private', 
                         redirect_uri = 'http://example.com/callback/'))
    playlistid_df = pd.read_csv(playlist_file)
    
    
    for pl_id in playlistid_df['playlist_id']:
        begin_time = datetime.now()
        results = sp.playlist(pl_id)
            #requests playlist data based on playlist_id 
        
        file_name = results['name'].lower().replace(' ','_') 
            #metadata for file naming
        
        songnum = 0
            #creates song number index for the month to be used in 'entry_id' calculation
            
        entry_id = []
        song_uri = []
        song_name = []
        artist_list = []
        playlist_name = []
            #establishes empty lists to be used in following for loop that creates dataframe of songs
        
        for i in range(len(results['tracks']['items'])):
            entry_id_calc = str(songnum) + '_' + file_name 
            entry_id.append(entry_id_calc)
            songnum = songnum + 1 
            song_location = results['tracks']['items'][i]['track']
            uri = song_location['uri']
            song_uri.append(uri)
            track_name = song_location['name']
            song_name.append(track_name)
            artist = song_location['artists'][0]['name']
            artist_list.append(artist)
            playlist_name.append(file_name)
                #this loop appends to lists that will be used to create the initial playlist dataframe
           
        
        df = pd.DataFrame(list(zip(entry_id, playlist_name, song_uri, song_name, artist_list)), 
                        columns =['entry_id','playlist_name','song_uri', 'song_name','artist'], index=None) 
        
        
        #converting the UTC time that was given by spotify into central time for each track:
        added_at = []
        to_zone = tz.gettz('America/Chicago')
        for i in range(len(results['tracks']['items'])):
            added = results['tracks']['items'][i]['added_at']
            utc_time = datetime.strptime(added, '%Y-%m-%dT%H:%M:%S%z')
            central = utc_time.astimezone(to_zone)
            added_at.append(central)    
        
        #creating lists to be used for additional in the playlist dataframe:
        duration = []       
        for i in range(len(results['tracks']['items'])):
            dur = results['tracks']['items'][i]['track']['duration_ms']
            duration.append(dur) 
        
        popularity = []       
        for i in range(len(results['tracks']['items'])):
            pop = results['tracks']['items'][i]['track']['popularity']
            popularity.append(pop) 
        
        #adding new columns to the playlist dataframe: 
        df["added_at"] = added_at
        df['add_month'] = df['added_at'].dt.month
        df['add_day'] = df['added_at'].dt.day
        df['add_time'] = df['added_at'].dt.time
        df['add_year'] = df['added_at'].dt.year
        df['added_at'] = df['added_at'].astype(str).str[:-6]
            #converting added_at from a datetime object to a string in the 
            #following format: '10:00PM'
        df["duration"] = duration
        df['popularity'] = popularity
    
        #creating placeholder variables for all audio stats:
        df['energy'] = 0
        df['danceability'] = 0
        df['key'] = 0
        df['loudness'] = 0
        df['mode'] = 0
        df['speechiness'] = 0
        df['acousticness'] = 0
        df['instrumentalness'] = 0
        df['liveness'] = 0
        df['valence'] = 0
        df['tempo'] = 0
        
        #replacing the placeholder values with actual audio statistics:
        for i in range(len(df)):
            audio_features = sp.audio_features(df['song_uri'][i])[0]
                #making a request to spotify for audio statistics of each song 
            df.loc[i,'energy'] = audio_features['energy']  
            df.loc[i,'danceability'] = audio_features['danceability']
            df.loc[i,'key'] = audio_features['key']
            df.loc[i,'loudness'] = audio_features['loudness']
            df.loc[i,'mode'] = audio_features['mode']
            df.loc[i,'speechiness']= audio_features['speechiness']
            df.loc[i,'acousticness'] = audio_features['acousticness']
            df.loc[i,'instrumentalness'] = audio_features['instrumentalness']
            df.loc[i,'liveness'] = audio_features['liveness']
            df.loc[i,'valence'] = audio_features['valence']
            df.loc[i,'tempo'] = audio_features['tempo']
        
            
        #checking for errors:
        name_error = 0
        artist_error = 0
        energy_error = 0
        dance_error = 0 
        mode_error = 0
        speech_error = 0 
        acous_error = 0
        instrumental_error = 0
        
        for i in range(len(df.index)):
            track_info = sp.track(df['song_uri'][i])
            audio_features = sp.audio_features(df['song_uri'][i])[0] 
        
            if track_info['name'] != df['song_name'][i]:
                name_error = name_error + 1    
                
            if track_info['artists'][0]['name'] != df['artist'][i]: 
                artist_error = artist_error + 1
                
            if audio_features['energy'] != df['energy'][i]:
                energy_error = energy_error + 1
                
            if audio_features['danceability'] != df['danceability'][i]:
                dance_error = dance_error + 1
            
            if audio_features['mode'] != df['mode'][i]:
                mode_error = mode_error + 1
                
            if audio_features['speechiness'] != df['speechiness'][i]:
                speech_error = speech_error + 1
                
            if audio_features['acousticness'] != df['acousticness'][i]:
                acous_error = acous_error + 1
                
            if audio_features['instrumentalness'] != df['instrumentalness'][i]:
                instrumental_error = instrumental_error + 1
        
        filepath = "D:\\File Storage\\Personal Projects\\1 Python\\Spotify Database" + file_name + ".csv"
            #assigning a custom filename
        df.to_csv(filepath, encoding="utf-8") 
            #converting dataframe object to csv and exporting 
        del df
            #deleting dataframe to clean everything up
        print('\n')
        print(file_name,' completed in ',(datetime.now()-begin_time))
        print('Failures: ', (instrumental_error+acous_error+speech_error+mode_error+
                             dance_error+energy_error+artist_error+name_error))


exportPlaylists("D:\\File Storage\\Personal Projects\\1 Python\\Spotify database\\sept2018pl.csv")
               # "D:\\File Storage\\Personal Projects\\1 Python\\Spotify database\\pl_id_data.csv"
               
               
               
               
               
               
               
               
               
               