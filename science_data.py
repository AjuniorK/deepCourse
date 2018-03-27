# -*- coding: utf-8 -*-
from __future__ import division
from datetime import datetime 
import requests
from lxml import html, etree
import json
from textblob import TextBlob
import numpy as np

import pandas as pd

import matplotlib.pyplot as plt

from geopy.geocoders import Nominatim

from googletrans import Translator as TranslatorG

import warnings
warnings.filterwarnings('always')

pd.options.display.max_columns = 1000
pd.options.display.max_rows = 350
pd.options.display.width = 1200

api_key = ""

parameters = {"part": "snippet",
              "maxResults": 5,
              "order": "date",
              "pageToken": "",
              "publishedAfter": "2008-08-04T00:00:00Z",
              "publishedBefore": "2008-11-04T00:00:00Z",
              "q": "",
              "key": api_key,
              "type": "video",
              }
url = "https://www.googleapis.com/youtube/v3/search"

parameters["q"] = "Bolsonaro"
page = requests.request(method="get", url=url, params=parameters)
j_results = json.loads(page.text)
print (j_results)

parameters = {"part": "statistics",
              "id": "_v4CbPcQ_HA",
              "key": api_key,
              }
url = "https://www.googleapis.com/youtube/v3/videos"

page = requests.request(method="get", url=url, params=parameters)
j_results = json.loads(page.text)
#print (page.text)

def _search_list(q="", publishedAfter=None, publishedBefore=None, pageToken="", cities_loc=None, regionCode=None):
    parameters = {"part": "id,snippet",
                  "maxResults": 50,
                  "order": "viewCount",
                  "pageToken": pageToken,
                  "q": q,
                  "type": "video",
                  "key": api_key,
                  #"location": str(cities_loc[0])+", "+str(cities_loc[1]),
                  #"locationRadius": "300km",
                  #"regionCode": str(regionCode).upper()
                  }
    url = "https://www.googleapis.com/youtube/v3/search"
    
    #if publishedAfter: parameters["publishedAfter"] = publishedAfter
    #if publishedBefore: parameters["publishedBefore"] = publishedBefore
    #print(parameters)
    try:
        page = requests.request(method="get", url=url, params=parameters)
    except Exception as e:
        page = {'error_test': 'true'}
        #print(e)
        return page
    #print(json.loads(page.text))
    return json.loads(page.text)

def search_list(q="", publishedAfter=None, publishedBefore=None, max_requests=50, cities_loc=None, regionCode=None):
    more_results = True
    pageToken=""
    results = []
    #count = 0
    #for city in cities_loc:
    for counter in range(max_requests):
        j_results = _search_list(q=q, publishedAfter=publishedAfter, publishedBefore=publishedBefore, pageToken=pageToken, cities_loc=cities_loc, regionCode=regionCode)
        #print("222222222222222222222222")        
        #print(j_results) 
        items = j_results.get("items", None)
        #print(items) 
        if items:
            results += [item["id"]["videoId"] for item in j_results["items"]]
            if "nextPageToken" in j_results:
                pageToken = j_results["nextPageToken"]
            else:
                return results
        else:
            return results
        #count +=1
        #print(count)
    return results

def _video_list(video_id_list):
    parameters = {"part": "statistics",
                  "id": ",".join(video_id_list),
                  "key": api_key,
                  "maxResults": 50
                  }
    url = "https://www.googleapis.com/youtube/v3/videos"
    page = requests.request(method="get", url=url, params=parameters)
    #print(params)
    j_results = json.loads(page.text)
    #print(j_results)
    df = pd.DataFrame([item["statistics"] for item in j_results["items"]], dtype=np.int64)
    df["video_id"] = [item["id"] for item in j_results["items"]]
    
    parameters["part"] = "snippet"
    page = requests.request(method="get", url=url, params=parameters)
    j_results = json.loads(page.text)
    df["publishedAt"] = [item["snippet"]["publishedAt"] for item in j_results["items"]]
    df["publishedAt"] = df["publishedAt"].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.000Z"))
    df["date"] = df["publishedAt"].apply(lambda x: x.date())
    df["week"] = df["date"].apply(lambda x: x.isocalendar()[1])
    df["channelId"] = [item["snippet"]["channelId"] for item in j_results["items"]]
    df["title"] = [item["snippet"]["title"] for item in j_results["items"]]
    df["description"] = [item["snippet"]["description"] for item in j_results["items"]]
    df["channelTitle"] = [item["snippet"]["channelTitle"] for item in j_results["items"]]
    df["categoryId"] = [item["snippet"]["categoryId"] for item in j_results["items"]]    
    return df

def _comment_list(video_id_list):
    values = []
    for index in video_id_list:
        #print(index)
        parameters = {"part": "snippet",
                       "videoId": index,
                       "textFormat":"plainText",
                       "key": api_key
                       }
        url = "https://www.googleapis.com/youtube/v3/commentThreads"
        page = requests.request(method="get", url=url, params=parameters)
        j_results = json.loads(page.text)
        #print(j_results["items"])
        #print(json.dumps(j_results["snippet"], indent=4, sort_keys=True))
        if "items" in j_results:
            #print(j_results["items"])
            df = pd.DataFrame([item["snippet"] for item in j_results["items"]], dtype=np.int64)
            #df["comment"] = [item["snippet"]["topLevelComment"]["snippet"]["textDisplay"] for item in j_results["items"]]
            for item in j_results["items"]:
                df["comment"] = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                df["author"] = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
                df["likeCount"] = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                df["authorChannelUrl"] = item["snippet"]["topLevelComment"]["snippet"]["authorChannelUrl"]
            values.append(df)
        
    return pd.concat(values)

def comment_list(video_id_list):
    values = []
    count = 0    
    for index, item in enumerate(video_id_list[::50]):
        t_index = index * 50
        values.append(_comment_list(video_id_list[t_index:t_index+50]))
        count += 1
        if count == 150:
            break
    return pd.concat(values)

def video_list(video_id_list):
    values = []
    #print(video_id_list)
    for index, item in enumerate(video_id_list[::50]):
        t_index = index * 50
        print("333333333333333333333")
        values.append(_video_list(video_id_list[t_index:t_index+50]))
        #print(video_id_list)
        #print(values)
    return pd.concat(values)

def get_data(theme, publishedAfter, publishedBefore, cities, regionCode):
    results_list = []
    cities_loc = []
    geolocator = Nominatim()
    list_theme = [theme]
    for city in cities:
        location = geolocator.geocode(city, timeout=10)
        cities_loc.append([location.latitude, location.longitude])
    for city in cities_loc:
        for q in list_theme:
            results = search_list(q=q,
                                  publishedAfter=publishedAfter,
                                  publishedBefore=publishedBefore,
                                  max_requests=50,
                                  cities_loc=city,
                                  regionCode=regionCode)
            #print(results)
            stat_data_set = video_list(results)
            stat_data_set["candidate_name"] = q
            results_list.append(stat_data_set)
    data_set = pd.concat(results_list)
    #print(data_set)
    return data_set

def get_2008_data(candidates):
    return get_data(candidates, publishedAfter="2008-08-04T00:00:00Z", publishedBefore="2008-11-04T00:00:00Z")

def get_2010_data(candidates):
    return get_data(candidates, publishedAfter="2010-08-04T00:00:00Z", publishedBefore="2010-11-04T00:00:00Z")

def get_2012_data(candidates):
    return get_data(candidates, publishedAfter="2012-08-04T00:00:00Z", publishedBefore="2012-11-04T00:00:00Z")

def get_2014_data(candidates):
    return get_data(candidates, publishedAfter="2014-01-04T00:00:00Z", publishedBefore="2014-12-04T00:00:00Z")
    
def get_2015_data(candidates):
    return get_data(candidates, publishedAfter="2015-01-04T00:00:00Z", publishedBefore="2015-01-04T00:00:00Z")

def get_2016_data(candidates):
    return get_data(candidates, publishedAfter="2016-01-04T00:00:00Z", publishedBefore="2016-12-04T00:00:00Z")

def get_2017_data(candidates):
    return get_data(candidates, publishedAfter="2017-01-04T00:00:00Z", publishedBefore="2017-12-04T00:00:00Z")

def get_2018_data(candidates):
    return get_data(candidates, publishedAfter="2018-01-04T00:00:00Z", publishedBefore="2018-12-04T00:00:00Z")
    
def get_2014_2018_data(theme, cities, regionCode):
    return get_data(theme, publishedAfter="2016-08-04T00:00:00Z", publishedBefore="2018-01-04T00:00:00Z", cities=cities, regionCode=regionCode)

def get_theme_data(language, theme):
    cities = []
    if language == "de":
        cities = ["Berlin"]
    elif language == "it":
        cities = ["Rome", "Milan", "Naples", "Turim", "Napoles", "Genova", "Pisa"]
    elif language == "ru":
        print("aaaaaaa")
        cities = ["Moscow", "Samara", "Novosibirsk"]
    elif language == "ar":
        print("aaaaaaa")
        cities = ["Moscow", "Samara", "Novosibirsk"]
    elif language == "en":
        print("aaaaaaa")
        cities = ["New York", "Washington", "Los Angeles"]
    elif language == "pt":
        print("aaaaaaa")
        cities = ["New York", "Washington", "Los Angeles"]
    elif language == "ja":
        print("aaaaaaa")
        cities = ["New York", "Washington", "Los Angeles"]
    elif language == "el":
        print("aaaaaaa")
        cities = ["New York", "Washington", "Los Angeles"]
        
    translator = TranslatorG()
    translations = translator.translate(theme, dest=language)
    print(translations.text)
    print(cities)
    print(language)
    data = get_2014_2018_data(translations.text, cities, regionCode=language)
    #comments = comment_list(data["video_id"])
    #print(comments["comment"])
    return data

#candidates = ["Prasident Trump"] # Bolsonaro (Direita), Lula (Esquerda)
var_2014_ds = get_2016_data(candidates)

teste = get_theme_data("de","President Trump")
teste_ru_city = get_theme_data("ru","President Trump")
teste_ru_city2 = get_theme_data("ru","President Trump")
teste_arabe_ = get_theme_data("ar","President Trump")
teste_america = get_theme_data("en","President Trump")
teste_brasil = get_theme_data("pt","The President Trump")
teste_japao = get_theme_data("ja","The President Trump")
teste_grego = get_theme_data("el","The President Trump")

comments = comment_list(teste_ru_city2["video_id"])
comment_arab = comments
comment_jap = comments
comment_russ = comments

comments["comment"].to_csv('comment_italy_trump.csv')
comments.to_csv('comment_russia_trump.csv')

comments_teste_novo = comment_list(teste_ru_city2["video_id"])

print(len(comment_russ))
print(comment_russ["comment"])
comment_russ.to_csv("comment_russ_trump2.csv", encoding='utf-8-sig')


comments_teste_novo2 = comments
comment_russ = comment_russ.drop_duplicates(subset='comment', keep='first', inplace=False)

data = pd.read_csv('comment_english_trump2.csv', encoding='Latin-1')
data.to_csv('comment_italy_trump_cleanfile.csv', encoding='Latin-1')

data_clean_germany = pd.read_csv('comment_germany_trump_cleanfile.csv', encoding='Latin-1')
list(data)

translator = TranslatorG()
translations = translator.translate(data['top_comment'][500], dest='en')


detect = translator.detect('Πρόεδρος Τρούμπα')
print(detect)


def only_one_lang(data, lang):
    translator = TranslatorG()
    index = 0
    for comment in data['top_comment']:
        detect = translator.detect(comment)
        if detect.lang != lang:
            print(detect)
            #data = data.drop(data.index[index])
            data = data[data.top_comment != comment]
        print(index)
        index += 1
    return data

data = data.fillna('')
data.dropna()
data.strip()

data['comment'].str.strip()
from textblob import TextBlob
b = TextBlob(data['comment'].str.strip())
print(b.correct())
print(len(data['comment']))

array_comments = []

for i in range(len(array_comments)):
    testimonial = TextBlob(str(array_comments[i]))
    print(testimonial.sentiment)

print(type(array_comments[0]))
for i in range(len(data['comment'])):

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment
    except:
        return None

df['sentiment'] = df['text'].apply(sentiment_calc)

#data = data.rename(columns = {'comment_top':'top_comment', '0':'index'})

teste2['commentCount'] = teste2['commentCount'].convert_objects(convert_numeric=True)

table = pd.pivot_table(teste2, values=["commentCount", "favoriteCount", "dislikeCount", "likeCount", "viewCount"], aggfunc=np.sum, index="candidate_name")
table
    
var_2014_ds = var_2014_ds.fillna(0)
var_2014_ds['dislikeCount'] = var_2014_ds['dislikeCount'].convert_objects(convert_numeric=True)
var_2014_ds['likeCount'] = var_2014_ds['likeCount'].convert_objects(convert_numeric=True)
var_2014_ds['commentCount'] = var_2014_ds['commentCount'].convert_objects(convert_numeric=True)

table = pd.pivot_table(var_2014_ds, values=["commentCount", "favoriteCount", "dislikeCount", "likeCount", "viewCount"], aggfunc=np.sum, index="candidate_name")
table

for candidate, color in zip(candidates, ["r", "b"]):
    cand = var_2014_ds[var_2014_ds["candidate_name"]==candidate]
    by_date = cand["week"].value_counts()
    by_date = by_date.sort_index()
    dates = by_date.index
    plt.plot(dates, by_date.values, "-o", label=candidate, c=color, linewidth=2)
plt.legend(loc="best")
plt.ylabel("Videos Published")
plt.xlabel("Week")
plt.show()

for candidate, color in zip(candidates, ["r", "b"]):
    cand = var_2014_ds[var_2014_ds["candidate_name"]==candidate]
    by_date = pd.pivot_table(cand, index=["week"], values=["viewCount"], aggfunc="sum")
    by_date = by_date.sort_index()
    dates = by_date.index
    plt.plot(dates, by_date.values, "-o", label=candidate, c=color, linewidth=2)
plt.legend(loc="best")
plt.ylabel("Videos viewCount")
plt.xlabel("Week")
plt.show()

for candidate, color in zip(candidates, ["r", "b"]):
    cand = var_2014_ds[var_2014_ds["candidate_name"]==candidate]
    by_date = pd.pivot_table(cand, index=["week"], values=["likeCount"], aggfunc="sum")
    by_date = by_date.sort_index()
    dates = by_date.index
    plt.plot(dates, by_date.values, "-o", label=candidate, c=color, linewidth=2)
plt.legend(loc="best")
plt.ylabel("Videos likeCount")
plt.xlabel("Week")
plt.show()

for candidate, color in zip(candidates, ["r", "b"]):
    cand = var_2014_ds[var_2014_ds["candidate_name"]==candidate]
    by_date = pd.pivot_table(cand, index=["week"], values=["dislikeCount"], aggfunc="sum")
    by_date = by_date.sort_index()
    dates = by_date.index
    plt.plot(dates, by_date.values, "-o", label=candidate, c=color, linewidth=2)
plt.legend(loc="best")
plt.ylabel("Videos dislikeCount")
plt.xlabel("Week")
plt.show()

candidates = ["Temer", "Lula"] # Bolsonaro (Direita), Lula (Esquerda)
var_2014_ds = get_2014_2018_data(candidates)
var_2014_ds = var_2014_ds.fillna(0)
var_2014_ds['dislikeCount'] = var_2014_ds['dislikeCount'].convert_objects(convert_numeric=True)
var_2014_ds['likeCount'] = var_2014_ds['likeCount'].convert_objects(convert_numeric=True)
var_2014_ds['commentCount'] = var_2014_ds['commentCount'].convert_objects(convert_numeric=True)

table = pd.pivot_table(var_2014_ds, values=["commentCount", "favoriteCount", "dislikeCount", "likeCount", "viewCount"], aggfunc=np.sum, index="candidate_name")
table