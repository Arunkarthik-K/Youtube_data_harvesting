from youtube import YoutubeData
from mongodb import MongoDB
from dataConversion import DataConversion
from sql import MySQL
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import streamlit as st
import plotly.express as px

# Configure Streamlit GUI
st.set_page_config(layout='wide')

st.markdown('<h1 style="text-align: center;"><span style="color: red;">YouTube</span> Data Harvesting and Warehousing</h1>', unsafe_allow_html=True)

st.header('Collect a live data')

channelId = st.text_input('**Enter channel_id**', placeholder='Channel id')
apikey = st.text_input('**Enter google api key**', placeholder='API key', type='password')

st.write('**Note:-** *The collected data will be stored in the :green[MongoDB].*')

submit = st.button('**Store the data**')

mongodbObj = MongoDB()

# Retrieve youtube data
if submit is True:
    youtubeObj = YoutubeData()
    youtubeData = youtubeObj.main(channelId, apikey)

    # Insert/Update youtube data into MongoDB
    channelName = youtubeData['Channel_Details']['Channel_Name']
    mongodbObj.main(channelId, channelName, youtubeData)
    st.success("The data has been successfully stored in the MongoDB database")

st.header('Data migration')

collection, client = mongodbObj.connection()

# Collect all document names
documentNames = []

for document in collection.find():
    documentNames.append(document["Channel_Name"])

document_name = st.selectbox('**Select channel name from collected data**', options=documentNames, key='document_names')

st.write('**Note:-** *The selected channel data will be Migrate to :blue[MySQL] database.*')

migrate = st.button('**Migrate to MySQL**')

if migrate is True:

    # Retrieve the document with the specified name
    result = collection.find_one({"Channel_Name": document_name})
    client.close()

    engine = create_engine('mysql+mysqlconnector://root:1234@localhost/youtube_project', echo=False)
    channelId = result['_id']
    query = f"SELECT Channel_Id FROM channel WHERE Channel_Id = '{channelId}';"
    results = pd.read_sql(query, engine)

    if results.empty:

        dataConversionObj = DataConversion()

        # Channel data json to dataframe
        channelDF = dataConversionObj.channelDataFrame(result)

        # playlist data json to dataframe
        playlistDF = dataConversionObj.playlistDataFrame(result)

        # video data json to dataframe
        videoDF = dataConversionObj.videoDataFrame(result)

        # Comment data json to dataframe
        commentDF = dataConversionObj.commentDataFrame(result)

        # Data migration from MongoDB to MySQL
        mySqlObj = MySQL()
        mySqlObj.main(channelDF, playlistDF, videoDF, commentDF)

    else:

        st.warning("The data has already been stored in MySQL database")
        option = st.selectbox('Do you want to overwrite the data?', ('Yes', 'No'))

        if option == 'Yes':
            dataConversionObj = DataConversion()

            # Channel data json to dataframe
            channelDF = dataConversionObj.channelDataFrame(result)

            # playlist data json to dataframe
            playlistDF = dataConversionObj.playlistDataFrame(result)

            # video data json to dataframe
            videoDF = dataConversionObj.videoDataFrame(result)

            # Comment data json to dataframe
            commentDF = dataConversionObj.commentDataFrame(result)

            # Data migration from MongoDB to MySQL
            mySqlObj = MySQL()
            mySqlObj.main(channelDF, playlistDF, videoDF, commentDF)

# Analysis channel data
st.header('Analysis channel data')

# Select box creation for question
question = st.selectbox('**Select your Question**',
                        ('1. What are the names of all the videos and their corresponding channels?',
                         '2. Which channels have the most number of videos, and how many videos do they '
                         'have?',
                         '3. What are the top 10 most viewed videos and their respective channels?',
                         '4. How many comments were made on each video, and what are their '
                         'corresponding video names?',
                         '5. Which videos have the highest number of likes, and what are their '
                         'corresponding channel names?',
                         '6. What is the total number of likes and dislikes for each video, and what '
                         'are their corresponding video names?',
                         '7. What is the total number of views for each channel, and what are their '
                         'corresponding channel names?',
                         '8. What are the names of all the channels that have published videos in the '
                         'year 2022?',
                         '9. What is the average duration of all videos in each channel, and what are '
                         'their corresponding channel names?',
                         '10. Which videos have the highest number of comments, and what are their '
                         'corresponding channel names?'),
                        key='collection_question')

st.write('**Note:-** *Analysis a channel data based on the question selection.*')
st.write('')

# Create a connection to SQL
connectForQuestion = pymysql.connect(host='localhost', user='root', password='1234', db='youtube_project')
cursor = connectForQuestion.cursor()

# Question 01
if question == '1. What are the names of all the videos and their corresponding channels?':

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON "
        "channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
    result1 = cursor.fetchall()
    df1 = pd.DataFrame(result1, columns=['Channel Name', 'Video Name'])
    df1.index.name = 'S.No'
    df1.index += 1
    st.dataframe(df1)

# Question 02
elif question == '2. Which channels have the most number of videos, and how many videos do they have?':

    cursor.execute("SELECT Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
    result2 = cursor.fetchall()
    df2 = pd.DataFrame(result2, columns=['Channel Name', 'Video Count'])
    df2.index.name = 'S.No'
    df2.index += 1
    st.dataframe(df2)

    fig1 = px.bar(df2, y='Video Count', x='Channel Name', text_auto=True)
    st.plotly_chart(fig1, use_container_width=True)

# Question 03
elif question == '3. What are the top 10 most viewed videos and their respective channels?':

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON "
        "channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY "
        "video.View_Count DESC LIMIT 10;")
    result3 = cursor.fetchall()
    df3 = pd.DataFrame(result3, columns=['Channel Name', 'Video Name', 'View count'])
    df3.index.name = 'S.No'
    df3.index += 1
    st.dataframe(df3)

    fig2 = px.bar(df3, y='View count', x='Video Name', text_auto=True)
    st.plotly_chart(fig2, use_container_width=True)

# Question 04
elif question == '4. How many comments were made on each video, and what are their corresponding video names?':

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON "
        "channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
    result4 = cursor.fetchall()
    df4 = pd.DataFrame(result4, columns=['Channel Name', 'Video Name', 'Comment count'])
    df4.index.name = 'S.No'
    df4.index += 1
    st.dataframe(df4)

# Question 05
elif question == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON "
        "channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY "
        "video.Like_Count DESC;")
    result5 = cursor.fetchall()
    df5 = pd.DataFrame(result5, columns=['Channel Name', 'Video Name', 'Like count'])
    df5.index.name = 'S.No'
    df5.index += 1
    st.dataframe(df5)

# Question 06
elif question == ('6. What is the total number of likes and dislikes for each video, and what are their corresponding '
                  'video names?'):

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN "
        "playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id "
        "ORDER BY video.Like_Count DESC;")
    result6 = cursor.fetchall()
    df6 = pd.DataFrame(result6, columns=['Channel Name', 'Video Name', 'Like count', 'Dislike count'])
    df6.index.name = 'S.No'
    df6.index += 1
    st.dataframe(df6)

# Question 07
elif question == ('7. What is the total number of views for each channel, and what are their corresponding channel '
                  'names?'):

    cursor.execute("SELECT Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
    result7 = cursor.fetchall()
    df7 = pd.DataFrame(result7, columns=['Channel Name', 'Total number of views'])
    df7.index.name = 'S.No'
    df7.index += 1
    st.dataframe(df7)

    fig3 = px.bar(df7, y='Total number of views', x='Channel Name', text_auto=True, )
    st.plotly_chart(fig3, use_container_width=True)

# Question 08
elif question == '8. What are the names of all the channels that have published videos in the year 2022?':

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON "
        "channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE "
        "EXTRACT(YEAR FROM Published_date) = 2022;")
    result8 = cursor.fetchall()
    df8 = pd.DataFrame(result8, columns=['Channel Name', 'Video Name', 'Year 2022 only'])
    df8.index.name = 'S.No'
    df8.index += 1
    st.dataframe(df8)

# Question 09
elif question == ('9. What is the average duration of all videos in each channel, and what are their corresponding '
                  'channel names?'):

    cursor.execute(
        "SELECT channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS "
        "duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON "
        "playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
    result9 = cursor.fetchall()
    df9 = pd.DataFrame(result9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)'])
    df9.index.name = 'S.No'
    df9.index += 1
    st.dataframe(df9)

# Question 10
elif question == ('10. Which videos have the highest number of comments, and what are their corresponding '
                  'channel names?'):

    cursor.execute(
        "SELECT channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON "
        "channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY "
        "video.Comment_Count DESC;")
    result10 = cursor.fetchall()
    df10 = pd.DataFrame(result10, columns=['Channel Name', 'Video Name', 'Number of comments'])
    df10.index.name = 'S.No'
    df10.index += 1
    st.dataframe(df10)

# Close MySQL connection
connectForQuestion.close()
