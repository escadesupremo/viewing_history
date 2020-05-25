#!/usr/bin/env python
# coding: utf-8

# The following mini-project intends to use my own Netflix history to determine which limited series (and first seasons) we binge watched since opening my Netflix account and how fast we finished them. 

# In[136]:


import pandas as pd
import numpy as np
from collections import OrderedDict
from pandas.io.json import json_normalize
import requests
import http.client
get_ipython().system('conda install -c anaconda beautifulsoup4 --yes')
from bs4 import BeautifulSoup
get_ipython().system('conda install -c anaconda lxml --yes')
import requests
import urllib.request 

#Importing relevant libraries to work with during the project


# In[138]:


#Importing my downloaded Netflix CSV file into a Pandas Dataframe
data = pd.read_csv('https://raw.githubusercontent.com/escadesupremo/viewing_history/master/NetflixViewingHistory%20(4).csv')
#Splitting Title among ':' for enabling the extraction of Season information
split = data["Title"].str.split(":", n = 3, expand = True)
#Adding watched date information to dataframe
split['Date'] = data['Date']
#Sampling data
split.head(4)


# In order to further cleanse data, season information on both columns 1 and 2 need to be extracted.

# In[139]:


#Filtering with Seasons starting with 1
data_1= split[split[1].astype(str).str.contains(' Season 1')]
data_2 = split[split[2].astype(str).str.contains(' Season 1')]
#Renaming columns needed for better readability.
data_2 = data_2.rename(columns={0: 'Title', 2: 'Season'})
data_1 = data_1.rename(columns={0: 'Title', 1: 'Season'})
#Merging title parts back together.
data_2["Title"] = data_2["Title"] + ':' + data_2[1]

result = data_1.append(data_2, sort=False)
#Append one dataframe with the other
result = result[['Title', 'Season', 'Date']]
#Further weed out season starting with 1
result = result[result.Season == ' Season 1']
#Check the bottom 4 results of the dataframe
result.tail(4)


# In order to further refine the data, I will the add the total count (number of episodes watched) as an additional column and remove duplicates from the dataframe.

# In[140]:


#Applying date formatting to determine 'number of days watched'
result['Date'] =  pd.to_datetime(result['Date'], format='%m-%d-%y', infer_datetime_format=True)

#Determining the earliest and the latest date for episodes watched for each series
min_series = result.groupby('Title').Date.min()
min_series.rename('MinDate', inplace=True)

max_series = result.groupby('Title').Date.max()
max_series.rename('Max_Depths', inplace=True)

#And turning the results into a dataframe with calculating the number of days passed from the first to last time
minimum_data_frame = pd.concat([min_series, max_series], axis=1)
minimum_data_frame['New'] = minimum_data_frame['Max_Depths'] - minimum_data_frame['MinDate']
minimum_data_frame.head(4)


# In order to further this dataframe, we would need to define the amount of days spent watching each series.

# In[141]:


#Finally, merging the dataframes together to ensure clearer understanding.
merged_data = pd.merge(result, minimum_data_frame[['New']], on='Title', how='left')
merged_data = merged_data.sort_values(by=['New'], ascending=False)

#Clearing up formatting to make it look better.
merged_data = merged_data.rename(columns={'count': 'Episodes Watched', 'New': 'Number of Days Watched'})
merged_data = merged_data.drop_duplicates(subset='Title',keep="last")
#Drop date, as it is not needed anymore
merged_data = merged_data.drop(columns=['Date'])
#Sampling results
merged_data.head(5)


# Merging the two frames together to achieve further clarity.

# In[167]:


#Finally, merging the dataframes together to ensure clearer understanding.
merged_data = pd.merge(result, minimum_data_frame[['New']], on='Title', how='left')
merged_data = merged_data.sort_values(by=['New'], ascending=False)

#Clearing up formatting to make it look better.
merged_data = merged_data.rename(columns={'count': 'Episodes Watched', 'New': 'Number of Days Watched'})
merged_data = merged_data.drop_duplicates(subset='Title',keep="last")
merged_data


# In order to gain access to the total number of episodes for each season 1, the Open Movie Database (OMDB API) is used.

# First we need to turn our title column into a list, which then can be passed onto the API.

# In[143]:


#Putting the Title values from CSV into a list.
requested = result['Title'].values.tolist() 
#Removing all the duplicates from the list.
requested = list(OrderedDict.fromkeys(requested)) 


# In[144]:


apikey = '49911e5b'
payload = requested
results = []
Season = '1'
#Above the paramaters for calling API and below the function which calls the API for each element of the list.
for t in payload:
    res = requests.get('http://www.omdbapi.com/?t={}&apikey={}&Season={}'.format(t, apikey, Season))

    if res.status_code == 200:
        results.append(res.json())
    else: 
        print("Request to {} failed".format(t))
        
results[3] #Sampling the results of the API call


# In[145]:


dataframe_omdb = json_normalize(results) #Normalizing JSON into a dataframe.
dataframe_omdb.head(5) #Sampling the results.


# The number of episodes watched based on viewing history needs to be determined.

# In[146]:


#Creating column with count of episodes watched
result['Episodes Watched'] = result.groupby('Title')['Title'].transform('count')
episodes_watched = result[['Title', 'Episodes Watched']].copy()
episodes_watched = episodes_watched.drop_duplicates(subset='Title',keep="last")

#Merging together with the previously cleansed viewing history data
merged_data = pd.merge(merged_data, episodes_watched, on='Title', how='left')
merged_data = merged_data[['Title', 'Number of Days Watched', 'Episodes Watched']]

#Sampling results
merged_data.tail(3)


# In[147]:


#Turning episodes into a dataframe series and counting the number of episodes available for each Title.
data_episodecount = dataframe_omdb['Episodes'].apply(pd.Series)
data_episodecount = data_episodecount.count(axis=1)
data_episodecount = pd.DataFrame.from_dict(data_episodecount)

#Merging the two dataframes to ensure more clear understanding
merged_dataAPI = pd.concat([dataframe_omdb, data_episodecount], axis=1)
merged_dataAPI = merged_dataAPI[["Title", "Season", 0]]
merged_dataAPI = merged_dataAPI.rename(columns={0: 'Episodes'})
merged_dataAPI = merged_dataAPI[merged_dataAPI.Episodes != 0]

#Sampling results
merged_dataAPI.head(3)


# Finally merging all frames together to see which TV series have been finished and view the number of days it took to finish it.

# In[148]:


merged_data_final = pd.merge(merged_data, merged_dataAPI, on='Title', how='left')
merged_data_final = merged_data_final.dropna()
merged_data_final['Episodes'] = merged_data_final['Episodes'].apply(int)
merged_data_final.tail(8)


# Lets see which TV series met the original criteria. "Episodes" with 1 total are not TV shows but movies and will be removed from the frame.

# In[149]:


merged_data_final2 = merged_data_final[merged_data_final['Episodes Watched'] == merged_data_final['Episodes']]
merged_data_final2 = merged_data_final2[merged_data_final2['Episodes'] != 1]
merged_data_final2


# Upon taking a closer look on this information, it seems that the Open Movie Database is not always correct. There are a number of instances where I have watched 1 more Season 1 episodes than what actually came out. Data is further inaccurate for a number of TV series with more than 1 episode, those are out of scope for this mini project.

# In[150]:


merged_data_final3 = merged_data_final[merged_data_final['Episodes Watched'] - 1 == merged_data_final['Episodes']]
merged_data_final3


# Lets take a look again on the API when we call it for the British series 'Bodyguard'.

# In[151]:


apikey = '49911e5b'
payload2 = ['Bodyguard']
results = []
Season = '1'
#Above the paramaters for calling API and below the function which calls the API for each element of the list.
for t in payload2:
    res = requests.get('http://www.omdbapi.com/?t={}&apikey={}&Season={}'.format(t, apikey, Season))

    if res.status_code == 200:
        results.append(res.json())
    else: 
        print("Request to {} failed".format(t))
        
results #Sampling the results of the API call


# As you can see on the results, Season 1 Episode 1 is not retrieved from the Database.

# In order to fix this issue, we will scrape the relevant Wikipedia websites for these series in order to determine the total number of Season 1 episodes. 

# In[152]:


test_link = requests.get('https://next-episode.net/bodyguard/season-1').text #Scraped URL
#Calling BeatifulSoup and looking for specific table
soup  = BeautifulSoup(test_link,'html.parser') 
tables = soup.find_all('div', style='background-color:#f8f8f8;padding:10px;')
#Putting results in a Pandas Frame
df = pd.read_html(str(tables))[0]
#Shaping dataframe to be fit for purpose
df.columns = df.iloc[0]
df = df.reindex(df.index.drop(0)).reset_index(drop=True)
df = df.iloc[0:6]
df["Episodes"] = df.shape[0]
#Renaming contents and Putting in Title in order to fit later data analysis
df = df.drop_duplicates(subset='Episodes',keep="last")
df = df.replace({'Episode Name': {'Episode 6': 'Bodyguard'}})
df = df.rename(columns={'Episode Name':'Title'})
df = df[['Title','Episodes']]
df


# In[153]:


test_link = requests.get('https://en.wikipedia.org/wiki/Trigger_Warning_with_Killer_Mike').text
#Calling BeatifulSoup and looking for specific table
soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders wikiepisodetable')
#Putting results in a Pandas Frame
df2 = pd.read_html(str(tables3))[0]

#Shaping dataframe to drop not needed data
df2 = df2.rename(columns={'Directed by':'Director'})
df2 = df2[df2.Director == 'Vikram Gandhi']
#Counting number of episodes
df2["Episodes"] = df2.shape[0]
#Further shaping data to be fit for purpose
df2 = df2.drop_duplicates(subset='Episodes',keep="last")
df2 = df2.replace({'Title': {'"Kill Your Master"': 'Trigger Warning with Killer Mike'}})
df2 = df2[["Title","Episodes"]]

df2


# In[154]:


test_link = requests.get('https://en.wikipedia.org/wiki/List_of_Friends_episodes').text
soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders wikiepisodetable')

df3 = pd.read_html(str(tables3))[0] 

df3["Episodes"] = df3.shape[0] #Episode Count

df3 = df3.drop_duplicates(subset='Episodes',keep="last")
df3 = df3.replace({'Title': {'"The One Where Rachel Finds Out"': 'Friends'}})
df3 = df3[["Title", "Episodes"]]

df3


# In[155]:


test_link = requests.get('https://en.wikipedia.org/wiki/Friends_from_College').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders')

df4 = pd.read_html(str(tables3))[0]

df4 = df4.rename(columns={'Season.1':'Title'})
df4 = df4[df4.Title == 1]
df4 = df4.replace({'Title': {1: 'Friends from College'}})
df4 = df4[['Title', 'Episodes']]

df4


# In[156]:


test_link = requests.get('https://en.wikipedia.org/wiki/The_Keepers').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders wikiepisodetable')

df5 = pd.read_html(str(tables3))[0]

val = ['1','2','3','4','5','6','7'] 
df5 = df5.loc[df5['No.'].isin(val)]
df5["Episodes"] = df5.shape[0] #Episode Count

df5 = df5.drop_duplicates(subset='Episodes',keep="last")
df5 = df5[['Title', 'Episodes']]
df5 = df5.replace({'Title': {'"The Conclusion"': 'The Keepers'}})

df5


# In[157]:


test_link = requests.get('https://en.wikipedia.org/wiki/On_My_Block_(TV_series)').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders')

df6 = pd.read_html(str(tables3))[0]

df6 = df6.rename(columns={'Season.1':'Title'})
df6 = df6[df6.Title == 1]
df6 = df6.replace({'Title': {1: 'On My Block'}})
df6 = df6[['Title', 'Episodes']]

df6


# In[158]:


test_link = requests.get('https://en.wikipedia.org/wiki/Champions_(American_TV_series)').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders wikiepisodetable')

df7 = pd.read_html(str(tables3))[0]

val = ['1','2','3','4','5','6','7','8','9','10']
df7 = df7.loc[df7['No.'].isin(val)]
df7["Episodes"] = df7.shape[0]

df7 = df7.drop_duplicates(subset='Episodes',keep="last")
df7 = df7[['Title', 'Episodes']]
df7 = df7.replace({'Title': {'"Deal or No Deal"': 'Champions'}})

df7


# In[159]:


test_link = requests.get('https://en.wikipedia.org/wiki/Dirty_Money_(2018_TV_series)').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders')

df8 = pd.read_html(str(tables3))[0]

df8 = df8.rename(columns={'Season.1':'Title'})
df8 = df8[df8.Title == 1]
df8 = df8.replace({'Title': {1: 'Dirty Money'}})
df8 = df8[['Title', 'Episodes']]

df8


# In[160]:


test_link = requests.get('https://en.wikipedia.org/wiki/Queer_Eye_(2018_TV_series)').text

soup  = BeautifulSoup(test_link,'html.parser')
tables3 = soup.find_all('table', class_='wikitable plainrowheaders')

df9 = pd.read_html(str(tables3))[0]

df9 = df9.rename(columns={'Season.1':'Title'})
df9 = df9[df9.Title == '1']
df9 = df9.replace({'Title': {'1': "Queer Eye: We're in Japan!"}})
df9 = df9[['Title', 'Episodes']]

df9


# And lets see together the actual count of episodes.

# In[161]:


dataframe = pd.concat([df, df2, df3, df4, df5, df6, df7, df8, df9], ignore_index=True) #Concatenating all dataframes together
dataframe = dataframe.rename(columns={'Episodes':'Actual Episode Count'}) #Differentiation needed for later merging
dataframe #See results


# In[162]:


merged_finalized = pd.merge(merged_data_final3, dataframe, on='Title', how='left') #Merging together the 2 frames

merged_finalized['Episodes'] = merged_finalized['Actual Episode Count'] #Episodes should be actual episode count
merged_finalized = merged_finalized.drop(columns=['Actual Episode Count']) #Drop the duplicated column
merged_finalized = merged_finalized[merged_finalized['Episodes Watched'] == merged_finalized['Episodes']] #Filter for results where we watched the whole episode

merged_finalized #Check results


# In[164]:


frames = [merged_finalized, merged_data_final2] #Defining mergable frames

result = pd.concat(frames) #Concatenating the 2 dataframes
result = result.sort_values(by=['Number of Days Watched'], ascending=True) #Sorting values to put the fastest ones on top

result.head(10) #Check the results


# I did double check and there are a number of TV series where the OMDB API is inaccurate. Hopefully once they add more episodes and information, running this notebook again will give 100% accurate results.

# And finally, lets see our top 3 finishers.

# <body>
#   <header>
#     <h2>Huge in France</h2>
#     <img src="http://www.impawards.com/tv/posters/huge_in_france.jpg", alt="Huge in France" width="150" height="75">
#     <h2>Trigger Warning with Killer Mike</h2>
#     <img src="https://upload.wikimedia.org/wikipedia/en/4/4a/Trigger_Warning_with_Killer_Mike_cover.jpg", alt="Trigger Warning with Killer Mike" width="150" height="75">
#     <h2>Kim's Convenience</h2>
#     <img src="https://static.wikia.nocookie.net/3f11ef62-865f-4e4f-8c01-02c80891c3d2/scale-to-width/600", alt="Huge in France" width="150" height="75">
#   </header>
# </body>
