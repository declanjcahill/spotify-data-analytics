# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 14:49:12 2022

@author: Declandre
"""

import sqlite3
import pandas as pd
from datetime import datetime

begin_time = datetime.now()

conn = sqlite3.connect('D:\\File Storage\\Personal Projects\\1 Python\\Spotify Database\\hub.db')
    #establishes a connection with the database

c = conn.cursor()
    #creates a cursor for running SQL commands, can use any variable name

playlistid_df = pd.read_csv("D:\\File Storage\\Personal Projects\\1 Python\\Spotify database\\pl_id_data.csv")

for pl_name in playlistid_df['playlist']:
    try:
        fun_begin_time = datetime.now()
        db_filepath = "D:\\File Storage\\Personal Projects\\1 Python\\Spotify database\\Months sheets4\\" + pl_name + ".csv"
        dftemp = pd.read_csv(db_filepath,index_col=0)
        
        dftemp.to_sql(pl_name, conn, if_exists='append', index=False)
            #THIS LINE OF CODE GETS RID OF UNNAMED COLUMN
            # Imports all db to hub
        del dftemp
        print('\n')
        print('completed ' + pl_name + ' at ' + str((datetime.now()-begin_time)) + "(in " + str((datetime.now()-fun_begin_time)) + " seconds)")
        print('\n')
    except:
        pass

conn.commit() 

c.execute("""CREATE TABLE master_db (
            
            entry_id TEXT,
            playlist_name TEXT,
            song_uri TEXT,
            song_name TEXT,
            artist TEXT,
            added_at TEXT,
            add_month INTEGER,
            add_day INTEGER,
            add_time TEXT,
            add_year INTEGER,
            duration INTEGER,
            popularity INTEGER,
            energy FLOAT,
            danceability FLOAT,
            key FLOAT,
            loudness FLOAT,
            mode FLOAT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT          
            )""")

#creates master datatable for all playlists

conn.commit()
playlistid_df = pd.read_csv("D:\\File Storage\\Personal Projects\\1 Python\\Spotify database\\pl_id_data.csv")

for pl_name in playlistid_df['playlist']:
    c = conn.cursor()
    if type(pl_name) == float:
        pass
    else:
        insertCommand = 'INSERT INTO master_db SELECT * FROM ' + pl_name
        c.execute(insertCommand)
        conn.commit()
    #need to have this line here to commit the updates
conn.close()
