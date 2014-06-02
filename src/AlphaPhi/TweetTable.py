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

            if retweetCount > 0 and 'object' in tweet and 'actor' in tweet['object']:
                if tweet['object']['id'][0 : len('tag:search.twitter.com,2005:')] == 'tag:search.twitter.com,2005:':
                    tweetid = tweet['object']['id'][len('tag:search.twitter.com,2005:') : ]
                    creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                    if 'followersCount' in tweet['object']['actor']:
                        num_followers = tweet['object']['actor']['followersCount']
                    timeDelta = retweetTime - creationTime
                    yield (int(tweetid),int(tweeter)), (int(user), int(num_followers), int(timeDelta.days * 86400 + timeDelta.seconds))

    def reducer(self, key, values):
        nf = 0
        store_t = [] # add no tweets if there aren't enough observations
        store_dt = []
        for tweeter, num_followers, dt in values:
            nf = max(nf, num_followers)
            if len(store_t) < MIN_OBSERVED_TWEETS:
                store_t.append(tweeter)
                store_dt.append(dt)
            else:
              yield None, ('%d %d %d %d %d ' % (key[0], key[1], tweeter, nf, dt))
              
        if len(store_t) == MIN_OBSERVED_TWEETS:
            yield None, ('%d %d %d %d 0 ' % (key[0], key[1], key[1], nf))
            for ttweeter, dt in zip(store_t,store_dt):
                yield None, ('%d %d %d %d %d ' % (key[0], key[1], ttweeter, nf, dt))


if __name__ == '__main__':
    TweetTable.run()
