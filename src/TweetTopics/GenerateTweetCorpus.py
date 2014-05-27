from mrjob.job import MRJob
import json as simplejson

TWEETID_PREFIX = 'tag:search.twitter.com,2005:'

class GenerateTweetCorpus(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def prepare_word(self,word):
        if word.startswith('@'):
            return False
        if word.startswith('#'):
            res = self.prepare_word(word[1:])
            if res:
                return ' '.join([res,res])

        if len(word) == 0:
            return False

        word = word.encode('ascii','ignore').translate(None,string.punctuation).lower()

        if len(word) == 0:
            return False

        if self.dict.check(word):
            return stem(word)
        return False

    def mapper_init(self):
        self.dict = enchant.Dict("en_US")

    def mapper(self, _, line):
        try:
            tweet = ujson.loads(line)
        except:
            tweet = {} #so skip this one
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en' and 'actor' in tweet :
            tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
            the_tweet = tweet['body'].split()
            result = []
            for i in xrange(len(the_tweet)):
                word = self.prepare_word(the_tweet[i])
                if word:
                    result.append(word)
            if 'object' in tweet and 'actor' in tweet['object'] and 'id' in tweet['object']['actor'] and 'retweetCount' in tweet and int(tweet['retweetCount']) > 0:
                tweetid = tweet['object']['id'][len('tag:search.twitter.com,2005:') : ]
            yield int(tweetid), ' '.join(result)

    def reducer(self, tweetid, tweets):
        for t in tweets:
            yield None, "%s\t%s" % (user,t)
            return

if __name__ == '__main__':
    GenerateTweetCorpus.run()
