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
            tweeterStatusesCount = tweet['object']['actor']['statusesCount'] # TODO: localize this by sampling it? how often have you tweeted RECENTLY?
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            yield None, ('%d %d %d ' % (int(user), int(tweeter) , int(tweeterStatusesCount))) # the space at the end is important!!!! #blackmagic


if __name__ == '__main__':
    MRFollowers.run()
