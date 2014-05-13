from mrjob.job import MRJob
import json as simplejson

USERID_PREFIX = 'id:twitter.com:'

class GenerateCorpus(MRJob):

    MAPPER_OUTPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def mapper(self, _, line):
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en' and 'actor' in tweet :
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            the_tweet = tweet['body'].split(' ')
            result = []
            for i in xrange(len(the_tweet)):
                if not the_tweet[i].startswith('@') && the_tweet[i] != 'RT' && not the_tweet[i].startswith('#'):
                    result.append(the_tweet[i].lower())
                if the_tweet[i].startswith('#'):
                    result.append(the_tweet[i][1:].lower())
                    result.append(the_tweet[i][1:].lower())
            yield int(user), ' '.join(result)
            if 'retweetCount' in tweet and int(tweet['retweetCount']) > 0:
                tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                yield int(tweeter), ' '.join(the_tweet)

    def reducer(self, user, tweets):
        yield None, "%s %s" % (user,' '.join(tweets))


if __name__ == '__main__':
    GenerateCorpus.run()
