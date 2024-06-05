''' API Client Methods '''

from enum import IntEnum
from typing import ItemsView

# Connect to YT API

import googleapiclient.discovery
api_service_name = "youtube"
api_version = "v3"
api_key = '<enter api key here>'

youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)

# API request for Channel

def get_channel_data(channel_id):
  # Prepare the API request
  request = youtube.channels().list(
      part="snippet,contentDetails,statistics,status",
      id=channel_id)
  # Execute the API request
  response = request.execute()
  item = response["items"][0]

  # Extract from the response, the channel attributes that we are interested in
  data={
     "id": channel_id,
     "name":item["snippet"]["title"],
     "type":item["kind"],
     "views":item["statistics"]["viewCount"],
     "description":item["snippet"]["description"],
     "status":item["status"]["privacyStatus"],
     "playlist_id":item["contentDetails"]["relatedPlaylists"]["uploads"]
  }
  return data

# API request for Playlists

def get_playlist_data(channel_id):
  #Prepare the API request
  request = youtube.playlists().list(
      part="snippet",
      channelId=channel_id
      )

  response = request.execute()
  items = response["items"]

  playlists = dict()
  for item in items:
    data = {
      "id":item["id"],
      "channel_id":item["snippet"]["channelId"],
      "name":item["snippet"]["title"]
            }
    playlists[data["id"]]=data

  return playlists


# API request for Comment

def get_commentthread_data(channel_id):
  #Prepare the API request
  request = youtube.commentThreads().list(
      part="snippet",
      allThreadsRelatedToChannelId=channel_id
      )

  response = request.execute()
  items = response["items"]

  commentthread = dict()
  for item in items:
    data=dict()
    data = {
      "id":item["id"],
      "video_id":item["snippet"]["videoId"],
      "text":item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
      "author":item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
      "publishdate":item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
    commentthread[data["id"]]=data
  return commentthread


# API request for Videos

def get_video_data(video_ids, channel):
  idstr=str()
  for video_id in video_ids:
    idstr+=video_id+','
  #Prepare the API request
  if len(idstr)>0:
    request = youtube.videos().list(
    part="snippet,statistics,contentDetails",
      id=idstr
      )
    response = request.execute()
    items = response["items"]
    videos = dict()
    for item in items:
      data = {
      "id":item["id"],
      "name":item["snippet"]["title"],
      "playlist_id":channel["playlist_id"],
      "channel_id":channel["id"],
      "description":item["snippet"]["description"],
      "publishdate":item["snippet"]["publishedAt"],
      "view_count":item["statistics"]["viewCount"],
      "like_count":item["statistics"]["likeCount"],
      "dislike_count":0,
      "favorite_count":item["statistics"]["favoriteCount"],
      "comment_count":item["statistics"]["commentCount"],
      "duration":item["contentDetails"]["duration"],
      "caption_status":item["contentDetails"]["caption"]
            }
      data["thumbnails"]=dict()
      for key in item["snippet"]["thumbnails"].keys():
        data["thumbnails"][key]=item["snippet"]["thumbnails"][key]["url"]
      videos[data["id"]] = data
  else:
    print(f"\nThere are no Videos/Video ID for this channel {channel_id}\n")
  return videos


# Connect with MySQL db
from google.cloud.sql.connector import Connector
import pymysql
import traceback
from sqlalchemy import create_engine

'''
  Connector for connecting to the Youtube channel information
'''

c=Connector()
conn_name='<enter connection name here>'
user='userID>'
password='<password>'
db='<db>'

# Initializing the Connector
def connection_helper():
  try:
    # dB Connection
    connection = c.connect(conn_name, "pymysql", user=user, password=password, db=db)
    print(f'''Successfully connected to database''')
    return connection
  except:
    print(f'''DB Connection Failed''')

# Get connection from pool
def get_connection():
  # Create a DB connection and connection
  pool=create_engine("mysql+pymysql://", creator=connection_helper)
  return pool.connect()


from operator import index
# Code for storing and retrieving objects from MySQL DB
import pandas as pd
import sqlalchemy

faqs = {
      'Q1':'What are the names of all the videos and their corresponding channels?',
      'Q2':'Which channels have the most number of videos, and how many videos do they have?',
      'Q3':'What are the top 10 most viewed videos and their respective channels?',
      'Q4':'How many comments were made on each video, and what are their corresponding video names',
      'Q5':'Which videos have the highest number of likes, and what are their corresponding channel names?',
      'Q6':'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
      'Q7':'What is the total number of views for each channel, and what are their corresponding channel names?',
      'Q8':'What are the names of all the channels that have published videos in the year 2022',
      'Q9':'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
      'Q10':'Which videos have the highest number of comments, and what are their corresponding channel names?'
    }


connection = get_connection()

'''
Convert Duration to Seconds
#formats are 'P2DT3H20M30S', 'PT3H20M30S','PT20M30S','PT30S'
'''
def convert_duration(duration):
  day=hour=mins=sec=0
  if duration != None:
    if 'D' in duration:
      day=duration.split('D',1)[0]
      if day is not None:
        day=day.split('P',1)[1]
    if 'H' in duration:
      hour=duration.split('H',1)[0]
      if hour is not None:
        hour= hour.split('T',1)[1]
    if 'M' in duration:
      mins=duration.split('M',1)[0]
      if mins is not None:
        if 'H' in mins:
          mins= mins.split('H')[1]
        else:
          if 'PT' in mins:
            mins=mins.split('PT')[1]
    if 'S' in duration:
      sec=duration.split('S',1)[0]
      if 'M' in sec:
        sec= sec.split('M')[1]
      else:
        if 'PT' in sec:
          sec=sec.split('PT')[1]
    #print(f'''d:{day} h:{hour} m:{mins} s:{sec}''')
    return (int(day)*86400 + int(hour)*3600 + int(mins)*60 + int(sec))


'''
Insert a Channel into dB
'''

def insert_channel(channel,docommit=True):
  sql = '''INSERT INTO channel (id,name,type,views,description,status,playlist_id) VALUES (:id,:name,:type,:views,:description,:status,:playlist_id);'''
  statement = sqlalchemy.text(sql)
  connection.execute(statement,{'id': channel['id'], 'name': channel['name'], 'type': channel['type'], 'views': channel['views'], 'description': channel['description'], 'status': channel['status'],'playlist_id':channel['playlist_id']})
  if docommit == True:
    connection.commit()

'''
Get a channel with given ID
'''
def get_channel(channel_id):
  sql = '''SELECT id,name,type,views,description,status,playlist_id FROM channel WHERE id=%(id)s;'''
  df=pd.read_sql_query(sql, connection, params={'id':channel_id})
  return df

'''
Get all channels
'''
def get_all_channels():
  sql = '''SELECT id,name FROM channel;'''
  df=pd.read_sql_query(sql, connection)
  return df

'''
Get all FAQs
'''
def get_all_faqs():
  return pd.DataFrame.from_dict(faqs, orient='index').reset_index().rename(columns={'index':'id',0:'question'})

'''
Insert Playlists into dB
'''
def insert_playlists(channel_id,playlists,docommit=True):
  sql = '''INSERT INTO playlist (id,channel_id,name) VALUES (:id,:channel_id,:name);'''
  statement = sqlalchemy.text(sql)
  for key in playlists.keys():
    playlist=playlists[key]
    connection.execute(statement,{'id':key, 'name': playlist['name'], 'channel_id': channel_id})
  if docommit == True:
    connection.commit()

'''
Get Playlists for a given Channel ID
'''
def get_playlists_by_channel(channel_id):
  sql = '''SELECT id,channel_id,name FROM playlist WHERE channel_id=%(channel_id)s;'''
  return pd.read_sql_query(sql, connection, params={'channel_id':channel_id})
'''
Insert Comments into dB
'''
def insert_comments(channel_id, comments,docommit=True):
  sql = '''INSERT INTO comment (id,video_id,author,published_date,comment_text) VALUES (:id,:video_id,:author,STR_TO_DATE(:published_date,"%Y-%m-%dT%H:%i:%sZ"),:comment_text);'''
  statement = sqlalchemy.text(sql)
  for key in comments.keys():
    comment=comments[key]
    connection.execute(statement,{'id': comment['id'],'video_id': comment['video_id'],'author': comment['author'],'published_date': comment['publishdate'],'comment_text':comment['text']})
  if docommit == True:
    connection.commit()
'''
Get Comments for a given Channel ID
'''
def get_comments_by_channel(channel_id):
  sql = '''SELECT id,video_id,comment_text,author,published_date FROM comment WHERE video_id IN (SELECT id FROM video WHERE channel_id=%(channel_id)s);'''
  return pd.read_sql_query(sql,connection, params={'channel_id':channel_id})

'''
Insert Videos for a given Channel ID
'''
def insert_videos(channel_id, videos,docommit=True):
  sql = '''INSERT INTO video (id,playlist_id,channel_id,name,description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,caption_status) VALUES (:id,:playlist_id,:channel_id,:name,:description,STR_TO_DATE(:published_date,"%Y-%m-%dT%H:%i:%sZ"),:view_count,:like_count,:dislike_count,:favorite_count,:comment_count,:duration,:caption_status);'''
  statement = sqlalchemy.text(sql)
  for key in videos.keys():
    video=videos[key]
    connection.execute(statement,{'id':video['id'],'playlist_id':video['playlist_id'],'channel_id':video['channel_id'],'name':video['name'],'description':video['description'],'published_date':video['publishdate'],'view_count':video['view_count'],'like_count':video['like_count'],'dislike_count':video['dislike_count'],'favorite_count':video['favorite_count'],'comment_count':video['comment_count'],'duration':convert_duration(video['duration']),'caption_status':video['caption_status']})
  if docommit == True:
    connection.commit()
'''
Get Videos for a given Channel ID
'''
def get_videos_by_channel(channel_id):
  sql = '''SELECT id,playlist_id,channel_id,name,description,published_date,view_count,like_count,dislike_count,favorite_count,comment_count,duration,caption_status FROM video WHERE video.playlist_id IN (SELECT playlist_id FROM channel WHERE channel_id=%(channel_id)s);'''
  return pd.read_sql_query(sql,connection, params={'channel_id': channel_id},index_col="id")

'''
   Commit all DATA at once
'''
def insert_all_info(channel, playlists, comments, videos):
  insert_channel(channel,False)
  insert_playlists(channel['id'],playlists,False)
  insert_videos(channel['id'], videos,False)
  insert_comments(channel['id'], comments,False)
  connection.commit()

'''
Get FAQs
'''
def get_answer(faq_id):
  sql = ''
  if faq_id == "Q1":
    sql = '''SELECT v.id video_id,v.name video_name,c.id channel_id,c.name channel_name FROM video v JOIN channel c ON (v.channel_id=c.id);'''
  elif faq_id == "Q2":
    sql = '''SELECT c.id channel_id,c.name channel_name,count(*) video_count FROM channel c JOIN video v ON (c.id=v.channel_id) GROUP BY channel_id,channel_name ORDER BY video_count DESC;'''
  elif faq_id == "Q3":
    sql = '''SELECT v.id video_id,v.name video_name,v.view_count view_count,c.id channel_id,c.name channel_name FROM video v JOIN channel c ON (v.channel_id=c.id) ORDER BY view_count DESC LIMIT 10;'''
  elif faq_id == "Q4":
    sql = '''SELECT v.id video_id, v.name video_name, comment_count FROM video v ORDER BY comment_count DESC;'''
  elif faq_id == "Q5":
    sql = '''SELECT v.id video_id,v.name video_name, v.like_count video_like_count, c.id channel_id,c.name channel_name FROM video v JOIN channel c ON (v.channel_id=c.id) ORDER BY video_like_count DESC LIMIT 1;'''
  elif faq_id == "Q6":
    sql = '''SELECT id video_id, name video_name, like_count video_like_count FROM video;'''
  elif faq_id =="Q7":
    sql = '''SELECT id,name,views FROM channel ORDER BY views DESC;'''
  elif faq_id == "Q8":
    sql = '''SELECT c.id channel_id,c.name channel_name, v.published_date published_yr FROM channel c JOIN video v ON (c.id=v.channel_id) WHERE YEAR(v.published_date)=2022;'''
  elif faq_id == "Q9":
    sql = '''SELECT c.id channel_id,c.name channel_name, AVG(v.duration) average_duration FROM channel c JOIN video v ON (c.id=v.channel_id) GROUP BY channel_id,channel_name ORDER BY average_duration DESC;'''
  elif faq_id == "Q10":
    sql = '''SELECT v.id video_id, v.name video_name, c.id channel_id, c.name channel_name, comment_count comment_count_top10 FROM video v JOIN channel c ON (c.id=v.channel_id) ORDER BY comment_count DESC LIMIT 10;'''
  #Test
  return pd.read_sql_query(sql,connection)

