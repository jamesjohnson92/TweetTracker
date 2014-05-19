from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
from stemming.porter2 import stem
import enchant
import string
import ujson

USERID_PREFIX = 'id:twitter.com:'

class GenerateCorpus(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def small_edit_distance(self,w1,w2,ed_max):
        if ed_max < 0:
            return False
        if w1 == "":
            return len(w2) <= ed_max
        if w2 == "":
            return len(w1) <= ed_max
        if w1[0] == w1[0]:
            return self.small_edit_distance(w1[1:],w2[1:],ed_max)
        else:
            return self.small_edit_distance(w1[1:],w2,ed_max-1) or self.small_edit_distance(w1,w2[1:],ed_max-1) or self.small_edit_distance(w1[1:],w2[1:],ed_max-1)

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

#        suggestions = self.dict.suggest(word)
#        if len(suggestions) > 0:
#            new_word = suggestions[0].lower()
#            if self.small_edit_distance(new_word,word,2):
#                return stem(new_word)
        return False


    def mapper_init(self):
        self.dict = enchant.Dict("en_US")

    def mapper(self, _, line):
        try:
            tweet = ujson.loads(line)
        except:
            tweet = {} #so skip this one
        if 'twitter_lang' in tweet and tweet['twitter_lang'] == 'en' and 'actor' in tweet :
            user = tweet['actor']['id'][len(USERID_PREFIX) :]
            the_tweet = tweet['body'].split()
            result = []
            for i in xrange(len(the_tweet)):
                word = self.prepare_word(the_tweet[i])
                if word:
                    result.append(word)
            yield int(user), ' '.join(result)
            if 'object' in tweet and 'actor' in tweet['object'] and 'id' in tweet['object']['actor'] and 'retweetCount' in tweet and int(tweet['retweetCount']) > 0:
                tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
                yield int(tweeter), ' '.join(result)

    def reducer(self, user, tweets):
        yield None, "%s\t%s" % (user,' '.join(tweets))


if __name__ == '__main__':
    GenerateCorpus.run()
