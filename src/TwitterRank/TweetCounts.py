from mrjob.protocol import JSONProtocol
from mrjob.step import MRStep

USERID_PREFIX = 'id:twitter.com:'

class MRTweetCounts(MRJob):

    INPUT_PROTOCOL = JSONProtocol  # read the same format we write

    def mapper(self, _, tweet):
	if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en' and 'actor' in tweet:
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            yield user, 1

    def reducer(self, key, vals):
        yield key, sum(vals)

        
if __name__ == '__main__':
    MRTweetCounts.run()
