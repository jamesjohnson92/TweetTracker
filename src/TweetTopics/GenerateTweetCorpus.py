from mrjob.job import MRJob
import json as simplejson

TWEETID_PREFIX = 'tag:search.twitter.com,2005:'

class GenerateTweetCorpus(MRJob):

    def mapper(self, _, line):
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en':
            tweet_id = tweet['id'][len(TWEETID_PREFIX) :]
            if 'retweetCount' in tweet and int(tweet['retweetCount']) > 0:
                tweet_id = tweet['object']['id'][len(TWEETID_PREFIX) :]
            yield int(tweet_id), tweet['body']

    def reducer(self, key, vals):
        for v in vals:
            yield key, v
            return #uniqueification

if __name__ == '__main__':
    GenerateTweetCorpus.run()

