from __future__ import print_function
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
import codecs 

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

FILE_NAME_ON_DRIVE = "covid_res_producer"

#google drive auth
gauth = GoogleAuth() 
gauth.LoadCredentialsFile("mycreds.txt")
if gauth.credentials is None:
    gauth.LocalWebserverAuth()
elif gauth.access_token_expired:
    gauth.Refresh()
else:
    gauth.Authorize()
gauth.SaveCredentialsFile("mycreds.txt")        
drive = GoogleDrive(gauth) 


def saveTweetToDrive(tweet):
    file_list = drive.ListFile({'q': "'1ari_FspX-VjCMjrGSr1RW6SXzy2P_9uQ' in parents and trashed=false"}).GetList()
    for file in file_list:
        print('title: %s, id: %s' % (file['title'], file['id']))
        if FILE_NAME_ON_DRIVE in file['title']:
            file = file_list[0]
            updated_content = file.GetContentString() + "\n" + tweet
            file.SetContentString(updated_content)
            file.Upload()
            print("File Updated")

ckey="jUUBNFSLTGbEqiwx22XJWGNT1"
csecret="8HyfIgzGDvgAZ79M4WWGBHa9zq0FavlPmBVyyuI19a785Nj0Wz"
atoken="740635692359950336-WC7WOx3BdMOD0XBMil32lmsbf7nQjYM"
asecret="kCajQoi9IQeoFewY2KOZkqzaqpD8mKiuOeIgoHINGqJj3"
 
class listener(StreamListener):
#count=0
    def on_data(self, data):
 
        #while (count<10):
            #count++;
        
            data =  data.encode("utf-8")
            data = data[:-2]
            datajson = json.loads(data)
            #print(datajson)
            tweet=datajson["text"]        
            #print(tweet + "\n")
            idt=datajson["id_str"]
            if tweet.find("RT ") == -1 :            
                screenName = datajson["user"]["screen_name"]
                savefile=idt
                save=open('twitterDB4.txt','a')
                save.write(savefile)
                save.write(" - ")
                save.write(screenName)
                save.write("\n")
                save.close()
                saveTweetToDrive(tweet)
            
            return(True)
 
    def on_error(self, status):
        print (status)
 
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["plasma"])