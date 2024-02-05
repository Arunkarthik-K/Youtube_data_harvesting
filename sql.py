import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine


class MySQL:

    def connection(self):

        connect = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234")

        # Create a new database and use
        myCursor = connect.cursor()
        myCursor.execute("CREATE DATABASE IF NOT EXISTS youtube_project")

        # Close the cursor and database connection
        myCursor.close()
        connect.close()

        # Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:1234@localhost/youtube_project', echo=False)

        return engine

    def mapChannelSql(self, channelDF, engine):

        channelDF.to_sql('channel', engine, if_exists='replace', index=False,
                         dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                                "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                "Video_Count": sqlalchemy.types.INT,
                                "Subscriber_Count": sqlalchemy.types.BigInteger,
                                "Channel_Views": sqlalchemy.types.BigInteger,
                                "Channel_Description": sqlalchemy.types.TEXT,
                                "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })

    def mapPlaylistSql(self, playlistDF, engine):

        playlistDF.to_sql('playlist', engine, if_exists='replace', index=False,
                          dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                                 "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })

    def mapVideoSql(self, videoDF, engine):

        videoDF.to_sql('video', engine, if_exists='replace', index=False,
                       dtype={'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                              'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                              'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                              'Video_Description': sqlalchemy.types.TEXT,
                              'Published_date': sqlalchemy.types.String(length=50),
                              'View_Count': sqlalchemy.types.BigInteger,
                              'Like_Count': sqlalchemy.types.BigInteger,
                              'Dislike_Count': sqlalchemy.types.INT,
                              'Favorite_Count': sqlalchemy.types.INT,
                              'Comment_Count': sqlalchemy.types.INT,
                              'Duration': sqlalchemy.types.VARCHAR(length=1024),
                              'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                              'Caption_Status': sqlalchemy.types.VARCHAR(length=225), })

    def mapCommentSql(self, commentsDF, engine):

        commentsDF.to_sql('comments', engine, if_exists='replace', index=False,
                          dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                                 'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                                 'Comment_Text': sqlalchemy.types.TEXT,
                                 'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                                 'Comment_Published_date': sqlalchemy.types.String(length=50), })

    def main(self, channelDF, playlistDF, videoDF, commentsDF):

        # Connect to the MySQL server
        engine = self.connection()

        # Process channel data to SQL
        self.mapChannelSql(channelDF, engine)

        # Process playlist data to SQL
        self.mapPlaylistSql(playlistDF, engine)

        # Process video data to SQL
        self.mapVideoSql(videoDF, engine)

        # Process commend data to SQL
        self.mapCommentSql(commentsDF, engine)
