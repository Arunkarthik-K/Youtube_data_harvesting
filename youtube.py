from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import streamlit as st
import re


class YoutubeData:

    def connection(self, apiKey):

        apiServiceName = 'youtube'
        apiVersion = 'v3'
        return build(apiServiceName, apiVersion, developerKey=apiKey)

    def getChannelData(self, youtube, channelId):
        try:
            channel_request = youtube.channels().list(
                part='snippet,statistics,contentDetails',
                id=channelId)

            channel_response = channel_request.execute()

            if 'items' not in channel_response:
                st.write(f"Invalid channel id: {channelId}")
                st.error("Enter the correct **channel_id**")
                return None

            return channel_response

        except HttpError as e:
            st.error('Server error, Please try again after a few minutes')
            st.write('An error occurred: %s', e)
            return None

    def mapChannelData(self, channelId, channelData):

        channel_name = channelData['items'][0]['snippet']['title']
        channel_video_count = channelData['items'][0]['statistics']['videoCount']
        channel_subscriber_count = channelData['items'][0]['statistics']['subscriberCount']
        channel_view_count = channelData['items'][0]['statistics']['viewCount']
        channel_description = channelData['items'][0]['snippet']['description']
        channel_playlist_id = channelData['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        # Format channel_data into dictionary
        channel = {
            "Channel_Details": {
                "Channel_Name": channel_name,
                "Channel_Id": channelId,
                "Video_Count": channel_video_count,
                "Subscriber_Count": channel_subscriber_count,
                "Channel_Views": channel_view_count,
                "Channel_Description": channel_description,
                "Playlist_Id": channel_playlist_id
            }
        }

        return channel

    def getVideoIds(self, youtube, channelPlaylistId):

        videoId = []
        next_page_token = None

        while True:

            # Get playlist items
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=channelPlaylistId,
                maxResults=50,
                pageToken=next_page_token)

            response = request.execute()

            # Get video IDs
            for item in response['items']:
                videoId.append(item['contentDetails']['videoId'])

            # Check if there are more pages
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        return videoId

    def getVideoData(self, youtube, videoIds):

        videoData = []
        for videoId in videoIds:

            # Get video details
            request = youtube.videos().list(
                part='snippet, statistics, contentDetails',
                id=videoId)

            response = request.execute()

            video = response['items'][0]

            # Retrieve video comments
            try:
                video['comment_threads'] = self.getVideoComments(youtube, videoId, maxComments=2)
            except:
                video['comment_threads'] = None

            # Duration format transformation
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')

            if duration != 'Not Available':
                duration = self.convertDuration(duration)

            video['contentDetails']['duration'] = duration

            videoData.append(video)

        return videoData

    def getVideoComments(self, youtube, videoId, maxComments):

        request = youtube.commentThreads().list(
            part='snippet',
            maxResults=maxComments,
            textFormat="plainText",
            videoId=videoId)

        response = request.execute()

        return response

    def convertDuration(self, duration):

        regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
        match = re.match(regex, duration)

        if not match:
            return '00:00:00'

        hours, minutes, seconds = match.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds

        return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60),
                                             int(total_seconds % 60))

    def mapVideoData(self, videoData):

        videos = {}
        for i, video in enumerate(videoData):

            videoId = video['id']
            videoName = video['snippet']['title']
            videoDescription = video['snippet']['description']
            tags = video['snippet'].get('tags', [])
            publishedAt = video['snippet']['publishedAt']
            viewCount = video['statistics']['viewCount']
            likeCount = video['statistics'].get('likeCount', 0)
            dislikeCount = video['statistics'].get('dislikeCount', 0)
            favoriteCount = video['statistics'].get('favoriteCount', 0)
            commentCount = video['statistics'].get('commentCount', 0)
            duration = video.get('contentDetails', {}).get('duration', 'Not Available')
            thumbnail = video['snippet']['thumbnails']['high']['url']
            captionStatus = video.get('contentDetails', {}).get('caption', 'Not Available')
            comments = 'Unavailable'

            # Handle case where comments are enabled
            if video['comment_threads'] is not None:
                comments = {}
                for index, comment_thread in enumerate(video['comment_threads']['items']):
                    comment = comment_thread['snippet']['topLevelComment']['snippet']
                    commentId = comment_thread['id']
                    commentText = comment['textDisplay']
                    commentAuthor = comment['authorDisplayName']
                    commentPublishedAt = comment['publishedAt']
                    comments[f"Comment_Id_{index + 1}"] = {
                        'Comment_Id': commentId,
                        'Comment_Text': commentText,
                        'Comment_Author': commentAuthor,
                        'Comment_PublishedAt': commentPublishedAt
                    }

            # Format processed video data into dictionary
            videos[f"Video_Id_{i + 1}"] = {
                'Video_Id': videoId,
                'Video_Name': videoName,
                'Video_Description': videoDescription,
                'Tags': tags,
                'PublishedAt': publishedAt,
                'View_Count': viewCount,
                'Like_Count': likeCount,
                'Dislike_Count': dislikeCount,
                'Favorite_Count': favoriteCount,
                'Comment_Count': commentCount,
                'Duration': duration,
                'Thumbnail': thumbnail,
                'Caption_Status': captionStatus,
                'Comments': comments
            }

        return videos

    def main(self, channelId, apiKey):

        # Access YouTube API
        youtube = self.connection(apiKey)

        # Retrieve channel data
        channelRawData = self.getChannelData(youtube, channelId)

        # Process the channel data
        channel = self.mapChannelData(channelId, channelRawData)

        # Retrieve video IDs from channel playlist
        playlistId = channel['Channel_Details']['Playlist_Id']
        videoIds = self.getVideoIds(youtube, playlistId)

        # Retrieve video data
        videoData = self.getVideoData(youtube, videoIds)

        # Process the video data
        videos = self.mapVideoData(videoData)

        # Combine channel data and videos data into a dict
        finalOutput = {**channel, **videos}

        return finalOutput
