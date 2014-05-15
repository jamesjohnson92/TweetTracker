from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob

USERID_PREFIX = 'id:twitter.com:'


class TweetTable(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    MAPPER_OUTPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, tweet):
        language = 'es'
        retweetCount = 0
        if 'retweetCount' in tweet :
            retweetCount = int(tweet['retweetCount'])
        if 'twitter_lang' in tweet :
            language = tweet['twitter_lang']

        if 'id' in tweet:
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            tweeter = user
            tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]

            yield None, ('%d %d %d ' % (int(tweetid), int(user), int(tweeter))) # the space at the end is important!!!! #blackmagic

            
            if language == 'en' and retweetCount > 0:
                tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
                yield None, ('%d %d %d ' % (int(tweetid), int(user), int(tweeter))) # the space at the end is important!!!! #blackmagic

if __name__ == '__main__':
    TweetTable.run()
