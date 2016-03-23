from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Please fill in your Twitter API keys
access_token = None
access_secret = None
api_key = None
api_secret = None

auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_secret)


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=str.encode)


class KafkaListener(StreamListener):
    def on_data(self, data):
        producer.send('tweets', data)
        print data
        return True


l = KafkaListener()
stream = Stream(auth, l)
stream.filter(track=['python', 'javascript', 'ruby'])
