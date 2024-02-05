import pymongo
import streamlit as st


class MongoDB:

    def connection(self):

        client = pymongo.MongoClient(
            'mongodb+srv://arunkarthikk97:Aruntech2024@cluster0.5ykpkeg.mongodb.net/?retryWrites=true&w=majority')
        mydb = client['youtube_project']
        collection = mydb['youtube_data']

        return collection, client

    def insertOrUpdate(self, channelId, channelName, youtubeData, client, collection):
        finalOutputData = {
            'Channel_Name': channelName,
            "Channel_data": youtubeData
        }

        upload = collection.replace_one({'_id': channelId}, finalOutputData, upsert=True)
        client.close()

        st.write(f"Updated document id: {upload.upserted_id if upload.upserted_id else upload.modified_count}")

    def main(self, channelId, channelName, youtubeData):

        # Create a client instance of MongoDB
        collection, client = self.connection()

        # Insert or update the YouTube data
        self.insertOrUpdate(channelId, channelName, youtubeData, client, collection)