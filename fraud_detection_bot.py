import tweepy
import logging
import time
import random
import requests

from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SERVICE_ACCOUNT = "keys.json"

SPREAD_SHEET_ID = '1fr0Y5Unj5QuZWk7tiaQYwv9KoO6FHDCLMSHgPm69ZLk'

def save_tweets_to_sheets(value, created_at, id, mentions_to_store):
    record = [[value, created_at, "https://twitter.com/twitter/statuses/" + str(id), str(id), str(mentions_to_store)]]
    print("Will try to save the tweet on the google sheets:", record)
    creds = None
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().append(spreadsheetId=SPREAD_SHEET_ID,
                            range="Sheet1!A:Z", 
                            valueInputOption="USER_ENTERED", 
                            body={"majorDimension":"ROWS", "values":record}
                        ).execute()
    print("Saved in sheets", result)

def extract_mentions_if_any(tweet):
    try :
        mentions = tweet['entities']['user_mentions']
        mentions_to_store = ""
        for mention in mentions:
            mentions_to_store += mention['screen_name'] + ','
        return mentions_to_store
    except Exception as e:
        print("Error occurred while extracting mentions, will ignore")
        return ""

def create_api():

    consumer_key = 'LxDLjQ0H7frZZhlvwpENYJxDm'
    consumer_secret = '2fxLcMwqVQWVOCPAYKwkFQ26kSZgBUk6T9my7uwO0SSfpgd1lL'
    access_token = '1388223049959297025-4vD3r36EmODYhc0gPH8rWcwzov7dX0'
    access_token_secret = 'lpasf8DnSYKg9ni3xXYs0uqe7lj6hLuNEhgyRJ6wFJ816'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    try:
        api.verify_credentials()
    except Exception as e:
        logging.error("Error creating API", exc_info=True)
        raise e
    print("API created")
    return api

def retweet(api, tweet):
    try:
        api.retweet(tweet['id'])
        print("Retweeted the parent tweet") 
    except tweepy.TweepError as e:
        print(e)

def tweet_contains_relevant_info(tweet):
    # print(tweet)
    fraud_keywords = ['fraud', 'beware', 'fake', 'fraudulent', 'scam', 'scammer', 'cheater', 'scamster', 'dupe', 'trick'] 
    keywords = ['o2', 'covid', 'test', 'medicine', 'cipla', 'oxygen', 'bed', 'beds', 'available', 'remdesivir', 'remidisivir', 'plasma', 'ventilator', 'ventilators', 'tocilizumab']
    return any(keyword in tweet.lower() for keyword in keywords) and any(keyword in tweet.lower() for keyword in fraud_keywords)

def call_sqs_proxy(username, created_at, text, link):
    try:
        url = 'https://ahxrilda6g.execute-api.ap-south-1.amazonaws.com/beta/enqueue/v1'
        headers = {'Content-type': 'application/json'}
        text=str(text).replace("\n", "")
        tweet = {'username': str(username), 'ts': str(created_at), 'text': text, 'link': str(link)}
        tweet = str(tweet).replace("'", '"')
        response = requests.post(url, data=tweet, headers=headers)
        if response.status_code == 200:
            print("Successfully called the gateway - it should be put in the SQS")
    except Exception as e:
        print("Error occurred, will ignore and continue", e)


def check_mentions(api, keywords, since_id):
    print("Retrieving mentions")
    new_since_id = since_id
    for tweet in tweepy.Cursor(api.mentions_timeline, since_id=since_id).items():
        new_since_id = max(tweet.id, new_since_id)

        if tweet_contains_relevant_info(tweet.text):
            print("Processing tweet with ID ", str(tweet.id))
            call_sqs_proxy(tweet.user.screen_name, tweet.created_at, tweet.text, "https://twitter.com/" + str(tweet.user.screen_name) + "/status/" + str(tweet.id))

        print("Now will check for parent tweet in case of tagging")
        parent_tweet_id = tweet.in_reply_to_status_id_str
        if parent_tweet_id is not None:
            parent_tweet = api.get_status(parent_tweet_id, tweet_mode='extended')._json
            if tweet_contains_relevant_info(parent_tweet['full_text']):
                print("Processing parent tweet with ID ", str(parent_tweet['id']))
                mentions_to_store = extract_mentions_if_any(parent_tweet)
                call_sqs_proxy(parent_tweet['user']['screen_name'], parent_tweet['created_at'], parent_tweet['full_text'], "https://twitter.com/" + str(parent_tweet['user']['screen_name']) + "/status/" + str(parent_tweet['id']))

    return new_since_id

def save_since_id(since_id):
    f = open("fraud_puller_since_id.txt", "w")
    f.write(str(since_id))
    f.close()
    print("Saved since_id")

def read_since_id():
    f = open("fraud_puller_since_id.txt", "r")
    return f.read()

def main():
    print("starting..")
    api = create_api()
    while True:
        try:
            since_id = check_mentions(api, ["", ""], int(read_since_id()))
            save_since_id(since_id)
            print("Waiting...")
            time.sleep(120)
        except Exception as e:
            logging.error("Error occurred, will ignore and continue", exc_info=True)
            time.sleep(300) 

main()