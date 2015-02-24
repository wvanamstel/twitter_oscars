# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from collections import Counter
from pymongo import MongoClient
from dateutil.parser import parse
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

def get_data_mongo():
    '''
    Connect to mongo data base and put data into a data frame for analysis
    IN: null
    OUT: data frame; collected tweet data
    '''
    # Connect to mongo database and retrieve data
    client = MongoClient()
    db = client.oscar
    collection = db.tweets
    
    # Store data in dataframe
    df = pd.DataFrame(list(collection.find()))
    
    # Parse data/time column and set index
    df.created_at = df.created_at.apply(lambda x: parse(x).strftime("%Y-%m-%d %H:%M:%S"))
    df.set_index(df.created_at, drop=False, inplace=True)
    
    return df

def analyse_data(df):
    '''
    Do analysis of tweet data
    IN: data frame; consisting of all collected tweet data
    OUT: stdout
    '''
    # Plot resampled frequency of tweets
    freq = pd.to_datetime(df.created_at)
    freq.index = freq
    resampled = freq.resample('10Min', how='count')
    resampled.plot(kind='bar', title='Frequency of tweets per 10 minutes', figsize=(8,5))
    
    #Find most active people tweeting
    users = df.user.apply(pd.Series)
    names = users.screen_name
    df['user_names'] = names    #add column of user names to use later
    names_cnt = Counter(names).most_common(n=10)
    print '\n'.join(['{0}\t{1}'.format(name, cnt) for name, cnt in names_cnt])

    # Remove 'trending???' bots
    ind = names.apply(lambda x: 'trending' not in x)
    filtered = names[ind]
    filtered_cnt = Counter(filtered).most_common(n=10)
    print '\n'.join(['{0}\t{1}'.format(name, cnt) for name, cnt in filtered_cnt])
    print 'Total number of users: ', len(set(list(names.values)))
    
    # Look at top tweeter 'clickphoto6000' activity (an actual person)
    user = 'clickphoto6000'
    cl_ph = df[df.user_names==user]
    freq = pd.to_datetime(cl_ph.created_at)
    freq.index = freq
    cl_ph_resamp = freq.resample('5Min', how='count')
    cl_ph_resamp.plot(kind='bar', title='Frequency of tweets by most active user', figsize=(8,5))
    
    # Investigating the peak in tweet activity reveals that John Legend was
    # performing 'Glory' at the time
    cl_ph.index = freq
    print cl_ph['2015-02-23 03:53:00':'2015-02-23 04:03:00'].text
    
    # Find most used hashtags
    raw_text = df.text.values
    hashtags = []
    for tweet in raw_text:
        for word in tweet.split():
            if word.startswith('#') and 'oscar' not in word.lower():
                hashtags.append(word.encode('utf-8'))
    hash_cnt = Counter(hashtags).most_common(n=10)
    print '\n'.join(['{0}\t{1}'.format(tag, cnt) for tag, cnt in hash_cnt])
    
    
def sentiment(df):
    '''
    Do sentiment analysis on tweets that were about the host
    IN: data frame; tweet data
    OUT: stdout
    '''
    # Find tweets referring to the evening's host
    ind = df.text.apply(lambda x: '#nph' in x.lower() or '#neilpatrickharris' in x.lower())    
    raw_text = df[ind].text
    sentiments = []
    for tweet in raw_text.values:
        blob = TextBlob(tweet)
        sentiments.append(blob.sentiment)
    
    # Plot summarized sentiment and frequency
    indx = pd.to_datetime(df[ind].index)
    sents = [x[0] for x in sentiments]
    sent_ts = pd.Series(sents, index=indx)
    sent_ts.resample('5Min', how='sum').plot(title='Total sentiment')
    sent_ts.resample('5Min', how='count').plot(kind='bar', title='Frequency of hash tags mentioning host')
    
    print raw_text['2015-02-23 02:44:01':'2015-02-23 02:44:20']

if __name__=='__main__':
    df = get_data_mongo()
    analyse_data(df)
    sentiment(df)