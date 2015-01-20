# -*- coding: utf-8 -*-
#testegithub
import csv
import pickle
import datetime
import sys

# load the pickle file
lis_statuses = []
str_filename = 'supertweets'
with open(str_filename + '.pickle', 'rb') as tweet_file:
    while True:
        try:
            status = pickle.load(tweet_file)
         
            # gets tweet info
            text = status["text"].replace('\n', ' ').replace('\r', ' ').replace('|','')
            tweet_id = str(status["id_str"])
            reply_to_tweet_id = status["in_reply_to_status_id_str"]
            reply_to_user = status["in_reply_to_screen_name"]
            retweeted = status["retweet_count"]
            favorited = status["favorite_count"]
            lang = status["lang"]
            created_at = status["created_at"]

            # gets source from tweet
            source = status["source"]
            head, sep, tail = source.partition('<')
            head, sep, tail = source.partition('>')
            source = tail.replace('</a>', ' ')

            # gets utc timestamp
            datetime_created_at = datetime.datetime.strptime(created_at, "%a %b %d %H:%M:%S +0000 %Y")
            timestamp = datetime_created_at.replace(tzinfo=datetime.timezone.utc).timestamp()

            # gets user info
            usr_name = status["user"]["screen_name"]
            usr_followers = status["user"]["followers_count"]
            usr_following = status["user"]["friends_count"]
            usr_favorited = status["user"]["favourites_count"]
            usr_listed = status["user"]["listed_count"]
            usr_tweeted = status["user"]["statuses_count"]
            usr_created_at = status["user"]["created_at"]
            usr_id = status["user"]["id_str"]
            usr_url = str(status["user"]["url"]).replace('\n', ' ').replace('\r', ' ').replace('|','')
            usr_profile_image = status["user"]["profile_image_url"]
            usr_description = status["user"]["description"].replace('\n', ' ').replace('\r', ' ').replace('|','')
            usr_lang = status["user"]["lang"]
            usr_location = status["user"]["location"].replace('\n', ' ').replace('\r', ' ').replace('|','')
            usr_time_zone = status["user"]["time_zone"]
            usr_default_profile = status["user"]["default_profile"]
            usr_default_profile_image = status["user"]["default_profile_image"]
            usr_protected = status["user"]["protected"]

            # gets place
            if status["place"]:
                place_name = status["place"]["name"]
                place_full_name = status["place"]["full_name"]
                place_id = status["place"]["id"]
                place_url = status["place"]["url"]
                place_country = status["place"]["country"]
                place_country_code = status["place"]["country_code"]
                place_type = status["place"]["place_type"]
            else:
                place_name = ''
                place_full_name = ''
                place_id = ''
                place_url = ''
                place_country = ''
                place_country_code = ''
                place_type = ''

            # gets coordinates
            if status["coordinates"]:
                longitude = status["coordinates"]["coordinates"][0]
                latitude = status["coordinates"]["coordinates"][1]
            else:
                longitude = ''
                latitude = ''

            # gets place's bounding box
#            if status["place"]["bounding_box"]:
#                bb_1 = status["place"]["bounding_box"]["coordinates"][0]
#                bb_2 = status["place"]["bounding_box"]["coordinates"][1]
#                bb_3 = status["place"]["bounding_box"]["coordinates"][2]
#                bb_4 = status["place"]["bounding_box"]["coordinates"][3]
#            else:
#                bb_1 = ''
#                bb_2 = ''
#                bb_3 = ''
#                bb_4 = ''

            # gets original tweet info
            if "retweeted_status" in status:
               original_id = status["retweeted_status"]["id_str"]
               original_text = status["retweeted_status"]["text"]
               original_created_at = status["retweeted_status"]["created_at"]
               original_user = status["retweeted_status"]["user"]["screen_name"]
            else:
               original_id = ''
               original_text = ''
               original_created_at = ''
               original_user = ''

            # gets entities info
            if "media" in status["entities"]:
               media = status["entities"]["media"]
            else:
               media = ''

            if "urls" in status["entities"]:
               urls = status["entities"]["urls"]
            else:
               urls = ''

            if "user_mentions" in status["entities"]:
               user_mentions = status["entities"]["user_mentions"]
            else:
               user_mentions = ''

            if "hashtags" in status["entities"]:
               hashtags_text = ''
               for i in range(len(status["entities"]["hashtags"])):
                  if i>1:
                     hashtags_text = '#'+str(status["entities"]["hashtags"][i]["text"])+'; '+hashtags_text
                  if i is 1:
                     hashtags_text = '#'+str(status["entities"]["hashtags"][i]["text"])+hashtags_text
                  if i is 0:
                     hashtags_text = ''

            # prepare data export
            temp_status = [text, tweet_id, reply_to_tweet_id, reply_to_user, retweeted, favorited, source, lang, latitude, longitude, created_at, int(timestamp), usr_name, usr_followers, usr_following, usr_listed, usr_tweeted, usr_favorited, usr_created_at, usr_id, usr_url, usr_profile_image, usr_description, usr_lang, usr_location, usr_time_zone, usr_default_profile, usr_default_profile_image, usr_protected, place_name, place_full_name, place_id, place_url, place_country, place_country_code, place_type, original_id, original_text, original_user, original_created_at, media, urls, user_mentions, hashtags_text]
            lis_statuses.append(temp_status)
            
        except Exception as E:
            print(E)
            break

# save file
with open(str_filename + '.csv', 'w', newline='', encoding="utf8") as csvfile:
    file_writer = csv.writer(csvfile, delimiter='|', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    file_writer.writerow(['text', 'tweet_id', 'reply_to_tweet_id', 'reply_to_user', 'retweeted', 'favorited', 'source', 'lang', 'latitude', 'longitude', 'created_at', 'timestamp', 'from_user', 'user_followers', 'user_following', 'user_listed', 'user_tweeted', 'user_favorited', 'user_created_at', 'user_id', 'user_url', 'user_profile_image', 'user_description', 'user_lang', 'user_location', 'user_time_zone', 'user_profile_default', 'user_image_default', 'user_protects_tweets', 'place_name', 'place_full_name', 'place_id', 'place_url', 'place_country', 'place_country_code', 'place_type', 'original_id', 'original_text', 'original_user', 'original_created_at', 'media', 'urls', 'user_mentions', 'hashtags'])
    for status in lis_statuses:
            file_writer.writerow(status)
