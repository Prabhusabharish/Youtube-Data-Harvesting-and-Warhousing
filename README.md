# YouTube Data Harvesting and Warehousing
Problem Statement:
The challenge at hand is to develop a user-friendly Streamlit application enabling users to access and analyze data from multiple YouTube channels. The application should encompass the following features:
1. Ability to input a YouTube channel ID and retrieve comprehensive data (Channel Name, Subscribers, Total Video Count, Playlist Id, Video Id, Likes, Dislikes, Comments for each Video) using a Google API Key.
2. Option to store the acquired data in a MongoDB database, essentially creating a data lake.
3. Capability to collect data from up to 10 different YouTube channels and effortlessly store them in the data lake with a simple button click.
4. Option to select a channel name and migrate its data from the data lake to a SQL database, organizing it into tables.
5. Ability to search and retrieve data from the SQL database using various search options, including joining tables to obtain detailed channel information.
Approach:
In essence, the overall approach entails constructing a straightforward user interface through Streamlit, retrieving data from the YouTube API Key, storing it in a MongoDB data lake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and seamlessly displaying the results within the Streamlit app. This streamlined approach ensures a user-friendly experience for data exploration and analysis across multiple YouTube channels.
