import pandas as pd


class DataConversion:

    def channelDataFrame(self, result):
        channelDetailsToSql = {
            "Channel_Name": result['Channel_Name'],
            "Channel_Id": result['_id'],
            "Video_Count": result['Channel_data']['Channel_Details']['Video_Count'],
            "Subscriber_Count": result['Channel_data']['Channel_Details']['Subscriber_Count'],
            "Channel_Views": result['Channel_data']['Channel_Details']['Channel_Views'],
            "Channel_Description": result['Channel_data']['Channel_Details']['Channel_Description'],
            "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
        }

        channelDF = pd.DataFrame.from_dict(channelDetailsToSql, orient='index').T

        return channelDF

    def playlistDataFrame(self, result):

        playlistToSql = {"Channel_Id": result['_id'],
                         "Playlist_Id": result['Channel_data']['Channel_Details']['Playlist_Id']
                         }
        playlistDF = pd.DataFrame.from_dict(playlistToSql, orient='index').T

        return playlistDF

    def videoDataFrame(self, result):

        videoDetailsList = []

        for i in range(1, len(result['Channel_data'])):

            videoDetailsToSql = {
                'Playlist_Id': result['Channel_data']['Channel_Details']['Playlist_Id'],
                'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                'Video_Name': result['Channel_data'][f"Video_Id_{i}"]['Video_Name'],
                'Video_Description': result['Channel_data'][f"Video_Id_{i}"]['Video_Description'],
                'Published_date': result['Channel_data'][f"Video_Id_{i}"]['PublishedAt'],
                'View_Count': result['Channel_data'][f"Video_Id_{i}"]['View_Count'],
                'Like_Count': result['Channel_data'][f"Video_Id_{i}"]['Like_Count'],
                'Dislike_Count': result['Channel_data'][f"Video_Id_{i}"]['Dislike_Count'],
                'Favorite_Count': result['Channel_data'][f"Video_Id_{i}"]['Favorite_Count'],
                'Comment_Count': result['Channel_data'][f"Video_Id_{i}"]['Comment_Count'],
                'Duration': result['Channel_data'][f"Video_Id_{i}"]['Duration'],
                'Thumbnail': result['Channel_data'][f"Video_Id_{i}"]['Thumbnail'],
                'Caption_Status': result['Channel_data'][f"Video_Id_{i}"]['Caption_Status']
            }
            videoDetailsList.append(videoDetailsToSql)

        videoDF = pd.DataFrame(videoDetailsList)

        return videoDF

    def commentDataFrame(self, result):

        commentDetailsList = []

        for i in range(1, len(result['Channel_data'])):

            commentsAccess = result['Channel_data'][f"Video_Id_{i}"]['Comments']

            if commentsAccess == 'Unavailable' or (
                    'Comment_Id_1' not in commentsAccess or 'Comment_Id_2' not in commentsAccess):

                commentDetailsToSql = {
                    'Video_Id': 'Unavailable',
                    'Comment_Id': 'Unavailable',
                    'Comment_Text': 'Unavailable',
                    'Comment_Author': 'Unavailable',
                    'Comment_Published_date': 'Unavailable',
                }

                commentDetailsList.append(commentDetailsToSql)

            else:
                for j in range(1, 3):

                    commentDetailsToSql = {
                        'Video_Id': result['Channel_data'][f"Video_Id_{i}"]['Video_Id'],
                        'Comment_Id': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Id'],
                        'Comment_Text': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Text'],
                        'Comment_Author': result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                            'Comment_Author'],
                        'Comment_Published_date':
                            result['Channel_data'][f"Video_Id_{i}"]['Comments'][f"Comment_Id_{j}"][
                                'Comment_PublishedAt'],
                    }

                    commentDetailsList.append(commentDetailsToSql)

        commentDF = pd.DataFrame(commentDetailsList)

        return commentDF
