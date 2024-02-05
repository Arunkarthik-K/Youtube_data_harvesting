# YouTube Data Harvesting and Warehousing

## Introduction 

* YouTube is a globally popular video-sharing platform, launched in 2005, that allows users to upload, share, and view a wide variety of content. With over 2 billion logged-in monthly users, it has become a cornerstone of online entertainment and education. Creators across genres, from vlogs to tutorials, leverage the platform's reach for audience engagement. YouTube's algorithm-driven recommendations contribute to its immersive user experience, making it a dynamic space for content discovery and community building. As a cultural phenomenon, YouTube continues to shape digital media and redefine the way we consume and produce content.

* This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

## Guideline:

### 1. Requirement Libraries

* sql
* sqlalchemy
* pymysql
* pandas
* streamlit
* plotly
* mysql
* pymongo
* googleapiclient
* re

### 2. ETL Process
#### ETL stands for Extract, Transform, Load, which is a data integration process used to transfer data from source systems to a target system in a structured and meaningful way.

#### a) Extract

* Gather data from various sources, such as databases, applications, or external APIs.

#### b) Transform

* Manipulate and restructure the extracted data to meet the target system's requirements or standards.
  
#### c) Load  

* Load the transformed data into the destination or target database for analysis, reporting, or other purposes.

## 3. User Guide

#### Step 1. Collect a live data

* Enter a **channel_id** and **google_api_key** in the text box and click the **Store the data** button to insert the data into **MongoDB**.

#### Step 2. Data migration

* Select the **channel name** and click the **Migrate to MySQL** button to migrate the specific channel data into the MySQL database.

#### Step 3. Analysis channel data

* **Select a Question** from the dropdown option you want and get the **results in tabular format**.
