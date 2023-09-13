
from googleapiclient.discovery import build
import pymongo
import psycopg2
import isodate
import pandas as pd
import streamlit as st


api_key = "AIzaSyB0wjCeupvbzYTv5KXJo5vIPAxvOkt7xpU"
api_service_name = "youtube"
api_version = "v3"
youtube = build(api_service_name, api_version, developerKey=api_key)

client = psycopg2.connect(host='localhost', user='postgres', password='Rajesh@123',
                             database='youtube')
cursor = client.cursor()

project = pymongo.MongoClient(
    "mongodb+srv://RAJESHK513:Rajeshkraji@cluster0.uf5udo7.mongodb.net/?retryWrites=true&w=majority")


st.set_page_config(layout="wide")


def youtube_channel(youtube, channel_id):
    request = youtube.channels().list(
        id=channel_id,
        part="snippet,contentDetails,statistics"
    )
    response = request.execute()

    for item in response['items']:
        data = {'channelName': item['snippet']['title'],
                'channel_id': item['id'],
                'subscribers': item['statistics']['subscriberCount'],
                'views': item['statistics']['viewCount'],
                'totalVideos': item['statistics']['videoCount'],
                'playlistId': item['contentDetails']['relatedPlaylists']['uploads'],
                'channel_description': item['snippet']['description']
                }

    return (data)


# # get play list details

def get_playlists(youtube, channel_id):
    request = youtube.playlists().list(
        part="snippet,contentDetails",
        channelId=channel_id,
        maxResults=25
    )
    response = request.execute()
    All_data = []
    for item in response['items']:
        data = {'PlaylistId': item['id'],
                'Title': item['snippet']['title'],
                'ChannelId': item['snippet']['channelId'],
                'ChannelName': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'VideoCount': item['contentDetails']['itemCount']
                }
        All_data.append(data)

        next_page_token = response.get('nextPageToken')

        while next_page_token is not None:
            request = youtube.playlists().list(
                part="snippet,contentDetails",
                channelId="UCmXkiw-1x9ZhNOPz0X73tTA",
                maxResults=25)
            response = request.execute()

            for item in response['items']:
                data = {'PlaylistId': item['id'],
                        'Title': item['snippet']['title'],
                        'ChannelId': item['snippet']['channelId'],
                        'PublishedAt': item['snippet']['publishedAt'],
                        'VideoCount': item['contentDetails']['itemCount']
                        }
                All_data.append(data)
            next_page_token = response.get('nextPageToken')

    return All_data


# # get video id

def channel_videoId(youtube, playlist_Id):
    video_Ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_Id,
        maxResults=50
    )
    response = request.execute()

    for i in range(len(response['items'])):
        video_Ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_Id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()

            for i in range(len(response['items'])):
                video_Ids.append(response['items'][i]['contentDetails']['videoId'])

            next_page_token = response.get('nextPageToken')
    return video_Ids


# # time conversion

def format_duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60

    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration


# # get individual video details

def video_details(youtube, video_Id):
    request = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        id=video_Id,
    )
    response = request.execute()
    video_info = {}
    for video in response['items']:
        stats_to_keep = {
            'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt', 'channelId'],
            'statistics': ['viewCount', 'likeCount', 'favoriteCount', 'commentCount'],
            'contentDetails': ['duration', 'definition', 'caption']
        }

        video_info['video_id'] = video['id']
        for key in stats_to_keep.keys():
            for value in stats_to_keep[key]:
                try:
                    if key == 'contentDetails' and value == 'duration':
                        video_info[value] = format_duration(video[key][value])
                    else:
                        video_info[value] = video[key][value]
                except KeyError:
                    video_info[value] = None

    return video_info


# # get comment details

def get_comments_in_videos(youtube, video_id):
    all_comments = []
    try:
        request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=video_id
        )
        response = request.execute()

        for item in response['items']:
            data = {'comment_id': item['snippet']['topLevelComment']['id'],
                    'comment_txt': item['snippet']['topLevelComment']['snippet']['textOriginal'],
                    'videoId': item['snippet']['topLevelComment']["snippet"]['videoId'],
                    'author_name': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt'],
                    }

            all_comments.append(data)

    except:

        return 'Could not get comments for video '

    return all_comments


# # import data to MongoDb
db = project["youtube_project"]
col = db["Channels"]

def channel_Details(channel_id):
    det = youtube_channel(youtube, channel_id)
    col = db["Channels"]
    col.insert_one(det)

    playlist = get_playlists(youtube, channel_id)
    col = db["playlists"]
    for i in playlist:
        col.insert_one(i)

    upload = det.get('playlistId')
    videos = channel_videoId(youtube, upload)
    for video in videos:
        v = video_details(youtube, video)
        col = db["videos"]
        col.insert_one(v)
        com = get_comments_in_videos(youtube, video)
        if com != 'Could not get comments for video ':
            for comment in com:
                col = db["comments"]
                col.insert_one(comment)

    return "Successfully to be store in mongodb"

def channel_table():
    try:
        cursor.execute("""create table if not exists channel 
                        (channelName varchar(50),
                        channelId varchar(100) primary key,
                        subscribers bigint,
                        views bigint, 
                        totalVideos int,
                        playlistId varchar(80),
                        channel_description text)"""
                       )
        client.commit()
    except:
        client.rollback()

    db = project["youtube_project"]
    col = db["Channels"]
    data = col.find()
    doc = list(data)
    df = pd.DataFrame(doc)
    try:
        for _, row in df.iterrows():  # iterate through each records
            insert_query = '''
                INSERT INTO channel (channelName, channelId, subscribers, views, totalVideos, playlistId, channel_description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            '''
            values = (
                row['channelName'],
                row['channel_id'],
                row['subscribers'],
                row['views'],
                row['totalVideos'],
                row['playlistId'],
                row['channel_description']
            )
            try:
                cursor.execute(insert_query, values)
                client.commit()
            except:
                client.rollback()

    except:
        st.write("values already exist in channels table")


# # table for playlist
def playlist_table():
    try:
        cursor.execute("""create table if not exists playlists
               (PlaylistId varchar(80) primary key,
                Title varchar(100),
                ChannelId varchar(100),
                ChannelName varchar(50),
                PublishedAt timestamp,
                VideoCount int)""")
        client.commit()
    except:
        project.rollback()

    col1 = db["playlists"]
    data1 = col1.find()
    doc1 = list(data1)
    df1 = pd.DataFrame(doc1)
    try:
        for _, raj in df1.iterrows():
            insert_query = '''
                            INSERT INTO playlists (PlaylistId, Title, ChannelId, ChannelName, PublishedAt, VideoCount)
                            VALUES (%s, %s, %s, %s, %s, %s)  '''

            values = (
                raj['PlaylistId'],
                raj['Title'],
                raj['ChannelId'],
                raj['ChannelName'],
                raj['PublishedAt'],
                raj['VideoCount']
            )
            try:
                cursor.execute(insert_query, values)
                client.commit()
            except:
                client.rollback()
    except:
        st.write("values already exist in playlist table")

    # # table for videos


def videos_table():
    try:
        cursor.execute("""create table if not exists videos
                        (video_id varchar(50)  primary key,
                        channelTitle varchar(150),
                        title varchar(150),
                        description text,
                        tags text,
                        publishedAt timestamp,
                        viewCount bigint,
                        likeCount bigint,
                        favoriteCount bigint,
                        commentCount int,
                        duration interval,
                        definition varchar(10),
                        caption varchar(10),
                        ChannelId varchar(100))""")
        client.commit()
    except:
        client.rollback()

    col2 = db["videos"]
    data2 = col2.find()
    doc2 = list(data2)
    df2 = pd.DataFrame(doc2)
    try:
        for _, esh in df2.iterrows():
            insert_query = '''
                INSERT INTO videos (video_id, channelTitle,  title, description, tags, publishedAt, 
                viewCount, likeCount, favoriteCount, commentCount, duration, definition, caption, channelId)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

            '''
            values = (
                esh['video_id'],
                esh['channelTitle'],
                esh['title'],
                esh['description'],
                esh['tags'],
                esh['publishedAt'],
                esh['viewCount'],
                esh['likeCount'],
                esh['favoriteCount'],
                esh['commentCount'],
                esh['duration'],
                esh['definition'],
                esh['caption'],
                esh['channelId']
            )
            try:
                cursor.execute(insert_query, values)
                client.commit()
            except:
                client.rollback()
    except:
        st.write("values aready exists in the videos table")


# # table for comments
def comments_table():
    try:
        cursor.execute("""create table if not exists comments
               (comment_id varchar(100)  primary key,
               comment_txt text,
               videoId varchar(80),
               author_name varchar(150),
               published_at timestamp)""")
        client.commit()
    except:
        project.rollback()

    col3 = db["comments"]
    data3 = col3.find()
    doc3 = list(data3)
    df3 = pd.DataFrame(doc3)
    try:
        for _, com in df3.iterrows():
            insert_query = '''
                INSERT INTO comments (comment_id, comment_txt, videoId, author_name, published_at)
                VALUES (%s, %s, %s, %s, %s)
                '''
            values = (
                com['comment_id'],
                com['comment_txt'],
                com['videoId'],
                com['author_name'],
                com['published_at']
            )
            try:
                cursor.execute(insert_query, values)
                client.commit()
            except:
                client.rollback()
    except:
        st.write("values already exist in comments table")



def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()
    return ("done")

st.title(":rainbow[YOUTUBE DATA HARVESTING AND WAREHOUSING]")

st.markdown(":grey[Enter The Channel ID From youtube]")
channel_ids = st.text_input("")
channel_ids = channel_ids.split(',')
channel_ids = [ch.strip() for ch in channel_ids if ch]


submit1=st.button("collect and store data in MongoDb")
if submit1:
    for channel in channel_ids:
        query = {'channel_id': channel}
        document = col.find_one(query)
        if document:
            st.write("channel details already exists")
    else:
        output = channel_Details(channel)
        st.write(output)
st.subheader(":rainbow[EXTRACT TO POSTGRESQL TABLE]")
st.caption("Mongodb to extract to store in sql table")
submit2=st.button("migrate data from MongoDb to Sql")
if submit2:
        display = tables()
        st.write(display)
st.subheader(":rainbow[Channel Data Analysis]")
st.caption("Qustions All Are Given Below")


query = st.selectbox("",("none", "What are the names of all the videos and their corresponding channels?",
                         "Which channels have the most number of videos, and how many videos do they have?",
                          "What are the top 10 most viewed videos and their respective channels?",
                         "How many comments were made on each video, and what are theircorresponding video names?",
                         "Which videos have the highest number of likes, and what are their corresponding channel names?",
                          "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                         "What is the total number of views for each channel, and what are their corresponding channel names?",
                         "What are the names of all the channels that have published videos in the year2022?",
                          "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                         "Which videos have the highest number of comments, and what are their corresponding channel names?"))
if query == "none":
        st.write("select an option")
if query == "What are the names of all the videos and their corresponding channels?":
    cursor.execute("select channeltitle as channel_name, title as videos from videos;")
    client.commit()
    t1 = cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["channel_name", "videos"]))
elif query == "Which channels have the most number of videos, and how many videos do they have?":
    cursor.execute(
        "select channelName as ChannelName,totalvideos as No_of_Videos from channel order by totalvideos desc;")
    client.commit()
    t2 = cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["ChannelName", "No_of_Videos"]))
elif query == "What are the top 10 most viewed videos and their respective channels?":
    cursor.execute('''select  channeltitle as ChannelName,title as video_title ,viewcount as views from videos 
                            where viewcount is not null order by viewcount desc limit 10;''')
    client.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns=["ChannelName", "video_title", "views"]))
elif query == "How many comments were made on each video, and what are theircorresponding video names?":
    cursor.execute("select title as Name , commentcount as No_of_comments  from videos where commentcount is not null;")
    client.commit()
    t4 = cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["Name", "No_of_comments"]))
elif query == "Which videos have the highest number of likes, and what are their corresponding channel names?":
    cursor.execute('''select channeltitle as ChannelName, title as Video_name, likecount as Likes from videos 
                       where likecount is not null order by likecount desc;''')
    client.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["ChannelName", "Video_name", "Likes"]))
elif query == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    cursor.execute('''select title as Video, likeCount as likes from videos;''')
    client.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Video", "likes"]))
elif query == "What is the total number of views for each channel, and what are their corresponding channel names?":
    cursor.execute("select channelName as ChannelName, views as Channelviews from channel;")
    client.commit()
    t7 = cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["ChannelName", "Channelviews"]))
elif query == "What are the names of all the channels that have published videos in the year2022?":
    cursor.execute('''select channeltitle as ChannelName , title as Video_name, publishedat as Posted_On from videos 
                       where extract(year from publishedat) = 2022;''')
    client.commit()
    t8 = cursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["ChannelName", "Video_name", "Posted_On"]))
elif query == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    cursor.execute(
        """SELECT channeltitle as ChannelName, AVG(duration) AS average_duration FROM videos GROUP BY channeltitle;""")
    client.commit()
    t9 = cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=["ChannelName", "average_duration"])
    tb9 = []
    for _, row in t9.iterrows():
        Channel_Name = row["ChannelName"]
        avg_duration = row["average_duration"]
        avg_str = str(avg_duration)
        tb9.append({"ChannelName": Channel_Name, "average_duration": avg_str})
    st.write(pd.DataFrame(tb9))
elif query == "Which videos have the highest number of comments, and what are their corresponding channel names?":
    cursor.execute('''select channelTitle as ChannelName, title as Video_name, commentCount as Comments from videos 
                       where commentCount is not null order by commentCount desc;''')
    client.commit()
    t10 = cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=["ChannelName", "Video_name", "Comments"]))

