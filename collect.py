# -*- coding: utf-8 -*-
from twython import Twython, TwythonRateLimitError
from datetime import datetime
from email.utils import parsedate
import time
import pickle
import sys

# fill this with your twitter API credentials
API_KEY = 'PZbG28uuEuh1A2YcF3wUg'
API_SECRET = 'NUhXaDszZD4PZZ8jT1XeZMu7VPcvSopEmFdplUESEs'

# write here your query terms. Maximum of 10.
# eg.: 'embratur OR "turismo brasil"' will match "Embratur, embratur" 
# "turismo brasil" will NOT match "turismo" or just "brasil"
query_terms = 'charlie'

# number of days to dig. Maximum of 7.
number_of_days = 1

# advanced, leave as it is unless you know what you are doing
max_id = None

# initialize connection with the twitter API
twitter = Twython(API_KEY, API_SECRET, oauth_version=2)
ACCESS_TOKEN = twitter.obtain_access_token()
twitter = Twython(API_KEY, access_token=ACCESS_TOKEN)

# function to ensure twitter limits are being respected
def application_rate_enforcer():
    try:
        rate_limit_status = twitter.get_application_rate_limit_status()['resources']['search']['/search/tweets']
        remaining = rate_limit_status['remaining']
    except:
        time.sleep(60)
    print('Requests remaining:', remaining)
    if remaining == 0:
        tts = rate_limit_status['reset'] - time.time() + 1
        print('Sleeping', int(tts), 'seconds')
        for i in range(3):
            time.sleep(0.4)
            print('.')
        time.sleep(tts)
    return

# create a smaller version of the tweet to save in a pickle file
def prepare_tweet_for_storage(dict_complete_tweet):
    dict_filtered_tweet = {
        'text': dict_complete_tweet['text'],
        'tweet_id': dict_complete_tweet['id_str'],
        'retweeted': dict_complete_tweet['retweet_count'],
        'favorited': dict_complete_tweet['favorite_count'],
        'source': dict_complete_tweet['source'],
        'coordinates': dict_complete_tweet['coordinates'],
        'lang': dict_complete_tweet['lang'],
        'created_at': dict_complete_tweet['created_at'],
        'reply_to_tweet_id': dict_complete_tweet['in_reply_to_status_id_str'],
        'reply_to_usr_name': dict_complete_tweet['in_reply_to_screen_name'],
        'retweeted_status': {
            'original_id': dict_complete_tweet['retweeted_status']['id_str'],
#            'original_text': dict_complete_tweet['retweeted_status']['text'],
            'original_created_at': dict_complete_tweet['retweeted_status']['created_at'],
            'original_user': dict_complete_tweet['retweeted_status']['user']['screen_name']
        },
        'user': {
            'usr_name': dict_complete_tweet['user']['screen_name'],
            'usr_followers': dict_complete_tweet['user']['followers_count'],
            'usr_following': dict_complete_tweet['user']['friends_count'],
            'usr_favorited': dict_complete_tweet['user']['favourites_count'],
            'usr_listed': dict_complete_tweet['user']['listed_count'],
            'usr_tweeted': dict_complete_tweet['user']['statuses_count'],
            'usr_created_at': dict_complete_tweet['user']['created_at'],
            'usr_id': dict_complete_tweet['user']['id_str'],
            'usr_url': dict_complete_tweet['user']['url'],
            'usr_profile_image': dict_complete_tweet['user']['profile_image_url'],
            'usr_description': dict_complete_tweet['user']['description'],
            'usr_lang': dict_complete_tweet['user']['lang'],
            'usr_location': dict_complete_tweet['user']['location'],
            'usr_time_zone': dict_complete_tweet['user']['time_zone'],
            'usr_default_profile': dict_complete_tweet['user']['default_profile'],
            'usr_default_profile_image': dict_complete_tweet['user']['default_profile_image'],
            'usr_protected': dict_complete_tweet['user']['protected']
        },
        'place': {
            'place_name': dict_complete_tweet['place']['name'],
            'place_full_name': dict_complete_tweet['place']['full_name'],
            'place_id': dict_complete_tweet['place']['id_str'],
            'place_url': dict_complete_tweet['place']['url'],
            'place_country': dict_complete_tweet['place']['country'],
            'place_country_code': dict_complete_tweet['place']['country_code'],
            'place_type': dict_complete_tweet['place']['place_type']
#            'place_bounding_box': dict_complete_tweet['places']['bounding_box']
        },
        'entities': {
            'media': dict_complete_tweet['entities']['media'],
            'urls': dict_complete_tweet['entities']['urls'],
            'user_mentions': dict_complete_tweet['entities']['user_mentions'],
            'hashtags': dict_complete_tweet['entities']['hashtags'],
        }
    }
    return dict_filtered_tweet

# count of total tweets
int_num_of_tweets = 0

# used to to check if the script is stuck
int_previous_num_of_tweets = 0
finished = False

# used to print periodically the progress of the search
int_time_to_print = 0

# initial created at used for error checking
created_at = None

# limit date is the utc timestamp of now minus the number of days specified in seconds
# one hour has 3600 seconds, a day has 24 hours...
limit_date = time.time() - int(24*3600*number_of_days)

# creates the datetime from the timestamp calculated above
datetime_limit = datetime.fromtimestamp(int(limit_date))

# language code is optional, but you can save a lot of time if you specify just one
# most common options here are:
# 'en' for english(all variations)
# 'pt' for portuguese(all variations)
# 'es' for spanish(all variations)
# all options here: http://en.wikipedia.org/wiki/List_of_ISO_639-1_codes

# open the pickle file
with open('supertweets.pickle', 'wb') as tweets_pickle_file:
    while True:
        application_rate_enforcer()
        print("Collecting...")
        try:
            while (True):
                result = twitter.search(q=query_terms, lang='', count=100, max_id=int(max_id)-1 if max_id else None)

                for status in result['statuses']:
                    last_result = status
                    int_num_of_tweets += 1
                    int_time_to_print += 1
                    pickle.dump((last_result), tweets_pickle_file)
                    
                max_id = last_result['id_str']
                str_created_at = last_result['created_at']
                datetime_created_at = datetime.strptime(str_created_at, "%a %b %d %H:%M:%S +0000 %Y")

#               prints the date about every 1000 tweets
                if int_time_to_print > 900:
                    print('Collected ', int_num_of_tweets, ' tweets. Oldest date is:', str_created_at, "id is:", max_id)
                    int_time_to_print = 0

                if (datetime_created_at < datetime_limit) or finished:
                    break

        except Exception as e:
            print(e)
            
        if (datetime_created_at < datetime_limit) or finished:
            if finished:
                print("Got all tweets available in the given period.")
            else:
                print("Reached date goal. Finishing.")
            print("Got ", int_num_of_tweets, "tweets.")
            print("Oldest id is: ", max_id)
            print("Min date is: ", str_created_at)

            break

        time.sleep(3)