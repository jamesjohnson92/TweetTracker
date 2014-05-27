from mrjob.protocol import JSONValueProtocol, RawValueProtocol
from mrjob.step import MRStep
from mrjob.job import MRJob

USERID_PREFIX = 'id:twitter.com:'


class MRFollowers(MRJob):

    INPUT_PROTOCOL = JSONValueProtocol
    MAPPER_OUTPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, tweet):
        retweetCount = 0
        language = 'es'
        if 'retweetCount' in tweet :
                retweetCount = int(tweet['retweetCount'])
        if 'twitter_lang' in tweet :
                language = tweet['twitter_lang']

        if language == 'en' and retweetCount > 0:
                tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                tweeterStatusesCount = tweet['object']['actor']['statusesCount']
                user = tweet['actor']['id'][len(USERID_PREFIX) :]
                yield (int(user), int(tweeter)) , int(tweeterStatusesCount)

    def reducer(self, key, value):
        user = key[0]
        tweeter = key[1]
        yield None, ('%d %d %d ' % (user, tweeter, sum(tweeterStatusesCount))) # the space at the end is important!!!! #blackmagic


if __name__ == '__main__':
    MRFollowers.run()
