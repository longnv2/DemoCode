import twitter

from urllib.parse import urlparse
import urllib

from pprint import pprint as pp

class TwitterAPI(object):
    """
    TwitterAPI class allows the Connection to Twitter via OAuth
    once you have registered with Twitter and receive the
    necessary credentiials
    """
    # initialize and get the twitter credentials
    def __init__(self):
        consumer_key = 'nlGgj6kTnZXsTiqqR16y7xRqy'
        consumer_secret = '2VnfKx9NqlFjKS8BWvcHu1jZdyvDFQNMt3FcnzrJ8nt94Kr6HX'
        access_token = '710358164781150208-d4740O0nnSBwyzEOio0u6ijSUs7pN9W'
        access_secret = '5oghrxX8QJJmn3o3e35ySuWoWRDzNx1jEcX4Rfy6u1Khx'
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_secret = access_secret
        #
        # authenticate credentials with Twitter using OAuth
        self.auth = twitter.oauth.OAuth(access_token, access_secret, consumer_key, consumer_secret)
        # creates registered Twitter API
        self.api = twitter.Twitter(auth=self.auth)
        #
        # search Twitter with query q (i.e. "ApacheSpark") and max. result
    
    def searchTwitter(self, q, max_res=10,**kwargs):
        search_results = self.api.search.tweets(q=q, count=10,**kwargs)
        statuses = search_results['statuses']
        max_results = min(1000, max_res)
        for _ in range(10):
            try:
                next_results = search_results['search_metadata']['next_results']
            except KeyError as e:
                break
            next_results = urllib.parse.parse_qsl(next_results[1:])
            kwargs = dict(next_results)
            search_results = self.api.search.tweets(**kwargs)
            statuses += search_results['statuses']
            if len(statuses) > max_results:
                break
        return statuses

    #
    # parse tweets as it is collected to extract id, creation
    # date, user id, tweet text
    def parseTweets(self, statuses):
        return [ (status['id'],
                status['created_at'],
                status['user']['id'],
                status['user']['name'],
                status['text'], url['expanded_url'])
                    for status in statuses
                    for url in status['entities']['urls']
    ]

t= TwitterAPI()

q="Apache Spark"

tsearch = t.searchTwitter(q)

print(len(tsearch))

pp(tsearch)


tparsed = t.parseTweets(tsearch)

pp(tparsed)