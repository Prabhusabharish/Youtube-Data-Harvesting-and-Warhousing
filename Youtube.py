import pandas as pd
from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from psycopg2 import IntegrityError




#API KEY Create
def Api_connect() :
    Api_key = "AIzaSyBBEZuTypwD3ge3zQ2rTFQ8RjqXxIXi07c"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey = Api_key)
    return youtube
youtube = Api_connect()  
#How to Get Channel Info:
def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
  )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i['snippet']['title'],
                Channel_Id=i["id"],
                Subscribers=i['statistics']["subscriberCount"],
                Views=i["statistics"]["viewCount"],
                Total_videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        ) 
        return data
  
def get_videos_ids(channel_id) :
    video_ids = []
    response = youtube.channels().list(id = channel_id,
                                    part = 'contentDetails').execute()
    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    while True :
        response1 = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_Id,
                                            maxResults = 50,
                                            pageToken = next_page_token).execute()
        for i in range(len(response1['items'])) :
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if next_page_token is None :
            break
    return video_ids

# HOW TO GET VIDEO INFORMATION
def get_video_info (video_ids) :
    video_data = []
    for video_id in video_ids :
        request = youtube.videos().list(
            part = "snippet,ContentDetails,statistics",
            id = video_id
        )
        response = request.execute()
        for item in response["items"] :
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Defination = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data



# How to get COMMENTS Information
def get_comment_info(video_ids) :
    Comment_data = []
    try :
        for video_id in video_ids :
            request = youtube.commentThreads().list(
                part = "snippet",
                videoId = video_id,
                maxResults = 50
                )
            response = request.execute()

            for item in response['items'] :
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                Comment_data.append(data)
    except :
        pass
    return Comment_data

# How to Get Playlist IDs
def get_playlist_details(channel_id) :
    next_page_token = None
    All_data = []
    while True :
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = next_page_token
        )
        response = request.execute()

        for item in response['items'] :
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        PublishedAt = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount'])
            All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None :
            break

    return All_data


client = pymongo.MongoClient("mongodb+srv://prabhusabharish:jamuna2011@test.xfart9q.mongodb.net/?retryWrites=true&w=majority")
db = client["Youtube_data"]

# IMPORT CHANNELS TO MONGODB
def channel_details(channel_id) :
    ch_details = get_channel_info(channel_id)
    pl_details = get_playlist_details(channel_id)
    vi_ids = get_videos_ids(channel_id)   
    vi_details = get_video_info (vi_ids)
    co_details = get_comment_info(vi_ids)

    coll1 = db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                     "video_information":vi_details,"comment_information":co_details})

    return "upload completed successfully"

# MONGODB TO SQL DATAFRAME CONVERT AND MIGRATE AND TABLES CREATE

# How to create channels and connect with SQL (Table creation)

def channels_table() :
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()


    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try :
        create_querry = '''create table if not exists channels(Channel_Name varchar(100),
                                                                Channel_Id varchar(100)  primary key,
                                                                Subscribers bigint,
                                                                Views bigint,
                                                                Total_videos int,
                                                                Channel_Description text,
                                                                playlist_Id varchar(80))'''
        
        cursor.execute(create_querry)
        mydb.commit()
    except :
        print("Channels table already created")

    # Mongodb to DATAFRAME CONVERT (CHANNELS)
    
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}) :
        ch_list.append(ch_data["channel_information"])

    df = pd.DataFrame(ch_list)

    # This is used 4 multi playlists to SQL
    for index,row in df.iterrows() :
            insert_query = '''insert into channels(Channel_Name,
                                                Channel_Id,
                                                Subscribers,
                                                Views,
                                                Total_videos,
                                                Channel_Description,
                                                Playlist_Id)
                                                        
                                                values(%s, %s, %s, %s, %s, %s, %s)'''
    
            values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Subscribers'],
                    row['Views'],
                    row['Total_videos'],
                    row['Channel_Description'],
                    row['Playlist_Id'],)
            
            try :
                cursor.execute(insert_query, values)
                mydb.commit()

            except :
                print("Channels values already inserted")
    

# How to create PLAYLISTS  and connect with SQL (Table creation)

def playlist_table() :
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()


    drop_query = '''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()

    
    create_querry = '''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            PublishedAt timestamp,
                                                            Video_Count int
                                                            )'''
    cursor.execute(create_querry)
    mydb.commit()

    # Mongodb to DATAFRAME CONVERT (PLAYLISTS)
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}) :
        for i in range(len(pl_data["playlist_information"])) :
            pl_list.append(pl_data["playlist_information"][i])

    df1 = pd.DataFrame(pl_list)

    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()


    # This is used 4 multi playlists to SQL
    for index,row in df1.iterrows() :
            insert_query = '''insert into playlists(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Video_Count
                                                )
        
                                                values(%s, %s, %s, %s, %s, %s)'''
    
            values = (row['Playlist_Id'],
                    row['Title'],
                    row['Channel_Id'],
                    row['Channel_Name'],
                    row['PublishedAt'],
                    row['Video_Count'],)
            
            cursor.execute(insert_query, values)
            mydb.commit()

# How to create VIDEOS and connect with SQL (Table creation)
def videos_table() :
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()


    drop_query = '''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_querry = '''create table if not exists videos(Channel_Name varchar(100),
                                                        Channel_Id varchar(100),
                                                        Video_Id varchar(30) primary key,
                                                        Title varchar(100),
                                                        Tags text,
                                                        Thumbnail varchar(200),
                                                        Description text,
                                                        Published_Date timestamp,
                                                        Duration interval,
                                                        Views bigint,
                                                        Likes bigint,
                                                        Comments int,
                                                        Favorite_Count int,
                                                        Defination varchar(10),
                                                        Caption_Status varchar(50)
                                                            )'''
    cursor.execute(create_querry)
    mydb.commit()


    # Mongodb to DATAFRAME CONVERT (PLAYLISTS)
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}) :
        for i in range(len(vi_data["video_information"])) :
            vi_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)

    # This is used 4 multi playlists to SQL
    for index,row in df2.iterrows() :
            insert_query = '''insert into videos(Channel_Name,
                                                    Channel_Id,
                                                    Video_Id,
                                                    Title,
                                                    Tags,
                                                    Thumbnail,
                                                    Description,
                                                    Published_Date,
                                                    Duration,
                                                    Views,
                                                    Likes,
                                                    Comments,
                                                    Favorite_Count,
                                                    Defination,
                                                    Caption_Status
                                                )
        
                                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

            values =(row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Defination'],
                    row['Caption_Status'],
                    )
            
            cursor.execute(insert_query, values)
            mydb.commit()

# How to create COMMENTS  and connect with SQL (Table creation)

def comments_table () :
    mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query = '''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query ='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                            Video_Id varchar(50),
                                                            Comment_Text text,
                                                            Comment_Author varchar(150),
                                                            Comment_Published timestamp
                                                            )'''

    cursor.execute(create_query)
    mydb.commit()

    # # Mongodb to DATAFRAME CONVERT (COMMENTS)
    co_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for co_data in coll1.find({},{"_id":0,"comment_information":1}) :
        for i in range(len(co_data["comment_information"])) :
            co_list.append(co_data["comment_information"][i])

    df3 = pd.DataFrame(co_list)

    # # This is the INSERT Query
    for index,row in df3.iterrows() :
            insert_query ='''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published
                                                )
        
                                                values(%s, %s, %s, %s, %s)'''

            values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published']
                    )
            
            cursor.execute(insert_query, values)
            mydb.commit()


# All SQL Tables creating in one clicks
def tables () :
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return "Tables Created Successfully"

Tables = tables()

def show_channels_table() :
    ch_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}) :
        ch_list.append(ch_data["channel_information"])

    df = st.dataframe(ch_list)

    return df

def show_playlists_table() :
    pl_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}) :
        for i in range(len(pl_data["playlist_information"])) :
            pl_list.append(pl_data["playlist_information"][i])

    df1 = st.dataframe(pl_list)

    return df1

def show_videos_table() :
    vi_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}) :
        for i in range(len(vi_data["video_information"])) :
            vi_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(vi_list)

    return df2

def show_comments_table() :
    co_list = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for co_data in coll1.find({},{"_id":0,"comment_information":1}) :
        for i in range(len(co_data["comment_information"])) :
            co_list.append(co_data["comment_information"][i])

    df3 = st.dataframe(co_list)

    return df3

# Streamlit part

with st.sidebar :
    st.title(":red[YOUTUBE DATA HARVESTING WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the Channel ID")

if st.button("Collect and Store Data") :
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_Id"])

    if channel_id in ch_ids :
        st.success("Channel Details of the given id is already exists")

    else :
        insert=channel_details(channel_id)
        st.success(insert)
        import streamlit as st
        st.balloons()

if st.button("Migrate to SQL") :
    Table = tables()
    st.success(Table)
    import streamlit as st
    st.snow()

show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS" :
    show_channels_table()

elif show_table == "PLAYLISTS" :
    show_playlists_table()

elif show_table == "VIDEOS" :
    show_videos_table()

elif show_table == "COMMENTS" :
    show_comments_table()

st.header(":rainbow[DATA ANALYSIS]", divider='green')

# SQL Connection

mydb = psycopg2.connect(host = "localhost",
                            user = "postgres",
                            password = "Sabharish2015",
                            database = "youtube_data",
                            port = "5432")
cursor = mydb.cursor()

question = st.selectbox("Select your Question", ("1. Names of all Videos and their corrs channel",
                                                 "2. A Channels have the most number of videos and Total no of  videos",
                                                 "3. The top 10 Most views videos and their channels",
                                                 "4. No of Comments in each videos and their corrs video",
                                                 "5. The highest liked videos and their corrs channels",
                                                 "6. Total no of Likes & dislikes video and their corrs videos",
                                                 "7. Total no of Views for each channel and their corrs channels",
                                                 "8. Videos published in the year of 2022",
                                                 "9. Average duration of all videos and their corrs channels",
                                                 "10. Videos with highest number of comments and their corrs channels"))

# Q & A = 1
mydb = psycopg2.connect(host = "localhost",
                        user = "postgres",
                        password = "Sabharish2015",
                        database = "youtube_data",
                        port = "5432")
cursor = mydb.cursor()

if question == "1. Names of all Videos and their corrs channel" :
    query1 = "select Title as videos, Channel_Name as ChannelName from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df = pd.DataFrame(t1, columns=["Video Title","Channel Name"])
    st.write(df)

# Q & A = 2
elif question == "2. A Channels have the most number of videos and Total no of  videos" :
    query2 = "select Channel_Name as ChannelName,Total_Videos as NO_Videos from channels order by Total_Videos desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2 = (pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))
    st.write(df2)

elif question == "3. The top 10 Most views videos and their channels" :
    query3 = '''select Views as views , Channel_Name as ChannelName,Title as VideoTitle from videos 
                        where Views is not null order by Views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns = ["views","channel Name","video title"])
    st.write(df3)

elif question == "4. No of Comments in each videos and their corrs video" :
    query4 = "select Comments as No_comments ,Title as VideoTitle from videos where Comments is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["No Of Comments", "Video Title"])
    st.write(df4)

elif question == "5. The highest liked videos and their corrs channels" :
    query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Likes as LikesCount from videos 
                       where Likes is not null order by Likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["video Title","channel Name","like count"])
    st.write(df5)

elif question == "6. Total no of Likes & dislikes video and their corrs videos" :
    query6 = '''select Likes as likeCount,Title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["like count","video title"])
    st.write(df6)

elif question == "7. Total no of Views for each channel and their corrs channels" :
    query7 = "select Channel_Name as ChannelName, Views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["channel name","total views"])
    st.write(df7)

elif question == "8. Videos published in the year of 2022" :
    query8 = '''select Title as Video_Title, Published_Date as VideoRelease, Channel_Name as ChannelName from videos 
                where extract(year from Published_Date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8 = pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"])
    st.write(df8)

elif question == "9. Average duration of all videos and their corrs channels" :
    query9 =  "SELECT Channel_Name as ChannelName, AVG(Duration) AS average_duration FROM videos GROUP BY Channel_Name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    df9 = pd.DataFrame(T9)
    st.write(df9)

elif question == "10. Videos with highest number of comments and their corrs channels" :
    query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comments as Comments from videos 
                       where Comments is not null order by Comments desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments'])
    st.write(df10)

