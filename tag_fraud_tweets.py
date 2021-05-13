import tweepy
import logging
import time
import random
import boto3

def create_api():

    consumer_key = 'LxDLjQ0H7frZZhlvwpENYJxDm'
    consumer_secret = '2fxLcMwqVQWVOCPAYKwkFQ26kSZgBUk6T9my7uwO0SSfpgd1lL'
    access_token = '1388223049959297025-4vD3r36EmODYhc0gPH8rWcwzov7dX0'
    access_token_secret = 'lpasf8DnSYKg9ni3xXYs0uqe7lj6hLuNEhgyRJ6wFJ816'

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)
    try:
        api.verify_credentials()
    except Exception as e:
        logging.error("Error creating API", exc_info=True)
        raise e
    print("API created")
    return api


def get_blacklisted_contacts_from_db():
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('BlackListedContacts')
    response = table.scan()
    contacts = response['Items']
    print("Contacts got from DB", contacts)
    return contacts

def search_tweets(api, since_id, max_items):
    tweets = []
    contacts = get_blacklisted_contacts_from_db()
    
    batch_size = 2
    batches = [contacts[i:i + batch_size] for i in range(0, len(contacts), batch_size)]
    print(batches)

    for batch in batches:
        query = generate_query(batch)
        for tweet in tweepy.Cursor(api.search,
                               q=query,
                               rpp=100,
                               since_id=since_id,
                               result_type="recent",
                               monitor_rate_limit=True,
                               wait_on_rate_limit=True,
                               include_entities=True,
                               lang="en").items(max_items):
            tweets.append(tweet)
    return tweets


def generate_query(batch):
    query, or_word = "", ""
    for contact in batch:
        query += or_word + str(contact['contact'])
        or_word = " OR "
        #query += " -filter:retweets AND filter:replies"
    print("Going to fire query : ", query)
    return query


def reply_to_fraud_tweets(api, tweets, dry=False):
    for tweet in tweets:
        write_to_file(str(tweet.id))
        write_to_file(tweet.text)
        if dry == True:
            print("Would have replied on tweet: ", tweet.text)
        else: 
            api.update_status(status="@"+tweet.user.screen_name+" This number is flagged for being fraudulent", in_reply_to_status_id=str(tweet.id), auto_populate_reply_metadata=True)

def save_since_id(since_id):
    f = open("last_since_id.txt", "w")
    f.write(str(since_id))
    f.close()
    print("Saved since_id")

def read_since_id():
    f = open("last_since_id.txt", "r")
    print("Read since_id ", f.read())
    return f.read()

def write_to_file(tweet):
    f = open("result.txt", "a")
    f.write(str(tweet))
    f.close()

def main():
    print("starting..")
    api = create_api()
    since_id = read_since_id()
    max_items = 1
    dry_mode = True

    while True:
        try:
            print("Since ID is ", since_id)
            tweets = search_tweets(api, since_id, max_items)
            if tweets is not None and len(tweets) > 0:
                tweets.sort(reverse=True, key=lambda tweet: tweet.id)
                since_id = tweets[0].id
                reply_to_fraud_tweets(api, tweets, dry=dry_mode)
                save_since_id(since_id)
            print("Waiting")
            time.sleep(120)
        except Exception as e:
            logging.error("Error occurred, will ignore and continue", exc_info=True)
            time.sleep(300) 

main()
