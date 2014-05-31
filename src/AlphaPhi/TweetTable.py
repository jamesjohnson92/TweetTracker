from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob
from datetime import datetime, timedelta


USERID_PREFIX = 'id:twitter.com:'

MIN_OBSERVED_TWEETS = 1 # only use E[num_retweets] >= 50, makes computing easier.  

class TweetTable(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, tweet):
        language = 'es'
        retweetCount = 0
        if 'retweetCount' in tweet :
            retweetCount = int(tweet['retweetCount'])
        if 'twitter_lang' in tweet :
            language = tweet['twitter_lang']

        if language == 'en' and 'id' in tweet:
            retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            num_followers = 0
            tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
            if retweetCount > 0:
                tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
                if 'followersCount' in tweet['object']['actor']:
                    num_followers = tweet['object']['actor']['followersCount']
            elif 'followersCount' in tweet['actor']:
                num_followers = tweet['actor']['followersCount']
            tweeter = user
            tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]

            if retweetCount > 0:
                creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                timeDelta = retweetTime - creationTime
                yield (int(user),int(tweetid)), (int(tweeter), int(num_followers), int((timeDelta.days * 86400 + timeDelta.seconds)))

    def reducer(self, key, values):
        nf = 0
        store = [] # add no tweets if there aren't enough observations
        for tweeter, num_followers in values:
            nf = max(nf, num_followers)
            if len(store) < MIN_OBSERVED_TWEETS:
                store.append(tweeter)
            else:
              yield None, ('%d %d %d %d ' % (key[0], key[1], tweeter, nf))
              
        if len(store) == MIN_OBSERVED_TWEETS:
            yield None, ('%d %d %d %d ' % (key[0], key[1], key[1], nf))
            for ttweeter in store:
                yield None, ('%d %d %d %d ' % (key[0], key[1], ttweeter, nf))


if __name__ == '__main__':
    TweetTable.run()
