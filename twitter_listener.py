# -*- coding: utf-8 -*-
"""
Created on Mon Feb 23 08:59:39 2015

@author: w
"""

import tweepy
import json
from settings import API_KEY, API_SECRET, ACCESS_TOKEN_KEY, ACCES_TOKEN_SECRET
from pymongo import MongoClient

'''
Listen to the Twitter firehose and store tweets into a mongo database
'''
class TwitterListener(tweepy.streaming.StreamListener):
    def __init__(self):
        # Set up mongo database
        db_client = MongoClient()
        db = db_client['oscar']
        self.collection = db['tweets']
        self.counter = 0            # number of tweets stored in DB
        
    def on_data(self, data):
        tweet = json.loads(data)    # convert json format tweet to dict
        if 'lang' in tweet and tweet['lang']=='en':
            self.counter += 1
            self.collection.insert(tweet)
            # Print some things to stdout temporarily
            if self.counter % 1000 == 0:            
                print self.counter
                print tweet['text']
        return True

    def on_error(self, error_code):
        # Print error code, but keep listening
        print 'error', error_code
        return True
        
    def on_timeout(self, status_code):
        # In case of time out keep listening
        print 'timeout', status_code
        return True
        
if __name__ == '__main__':   
    # Set up authentication
    auth = tweepy.OAuthHandler(API_KEY, API_SECRET)
    auth.set_access_token(ACCESS_TOKEN_KEY, ACCES_TOKEN_SECRET)
    
    # Construct stream instance
    listener = TwitterListener()
    stream = tweepy.Stream(auth, listener)
    stream.filter(track=['Oscars', 'Oscars2015'])
    #stream.sample()    # Just get everything, no filtering