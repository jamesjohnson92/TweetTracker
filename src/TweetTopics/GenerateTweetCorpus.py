from mrjob.job import MRJob
import json as simplejson

TWEETID_PREFIX = 'tag:search.twitter.com,2005:'

class GenerateTweetCorpus(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en':
            tweet_id = tweet['id'][len(TWEETID_PREFIX) :]

            the_tweet = tweet['body'].split(' ')
            result = []
            for i in xrange(len(the_tweet)):
                if not the_tweet[i].startswith('@') && the_tweet[i] != 'RT' && not the_tweet[i].startswith('#'):
                    result.append(the_tweet[i].lower())
                if the_tweet[i].startswith('#'):
                    result.append(the_tweet[i][1:].lower())
                    result.append(the_tweet[i][1:].lower())

            if 'retweetCount' in tweet and int(tweet['retweetCount']) > 0:
                tweet_id = tweet['object']['id'][len(TWEETID_PREFIX) :]
            yield int(tweet_id), ' '.join(the_tweet)

    def reducer(self, key, vals):
        for v in vals:
            yield None, "%d %s" % (key, v)
            return #uniqueification

if __name__ == '__main__':
    GenerateTweetCorpus.run()
