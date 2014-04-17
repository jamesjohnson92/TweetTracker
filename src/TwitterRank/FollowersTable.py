from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep


USERID_PREFIX = 'id:twitter.com:'


class MRFollowers(MRJob):

    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def mapper(self, _, tweet):
	retweetCount = 0
	language = 'es'
	if 'retweetCount' in tweet :
            retweetCount = tweet['retweetCount']
	if 'twitter_lang' in tweet :
            language = tweet['twitter_lang']

	if language == 'en' and retweetCount > 0:
            tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            yield user, tweeter

    def reducer(self, key, vals):
        for v in vals:
            yield key v

if __name__ == '__main__':
    MRFollowers.run()
