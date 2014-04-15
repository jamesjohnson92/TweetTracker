from mrjob.job import MRJob
import json as simplejson

USERID_PREFIX = 'id:twitter.com:'

class GenerateCorpus(MRJob):

    def mapper(self, _, line):
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en' and 'actor' in tweet :
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            yield user, tweet['body']

    def reducer(self, user, tweets):
        yeild user, ' '.join(tweets)

                
if __name__ == '__main__':
    GenerateCorpus.run()
