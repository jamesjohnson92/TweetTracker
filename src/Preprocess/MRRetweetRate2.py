from mrjob.job import MRJob
import json as simplejson
from mrjob.protocol import RawValueProtocol
from datetime import datetime, timedelta

class MRRetweetRate(MRJob):
	

    OUTPUT_PROTOCOL = RawValueProtocol


    PARTITION_SECS = 60 # bin things in five minutes
    NUM_PARTITIONS = 60*60/PARTITION_SECS # record for an hour

    def mapper(self, _, line):
        startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        endTime = datetime.strptime('2014-03-01T02:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet : 
            language = tweet['twitter_lang']
            if language == 'en' :
                if 'id' in tweet and 'retweetCount' in tweet :
                    tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
                    retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    creationTime = retweetTime
                    favoritesCount = tweet['favoritesCount']
                    retweetCount = tweet['retweetCount']
                    if tweet['retweetCount'] > 0 :
                        tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
                        creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        favoritesCount = tweet['object']['favoritesCount']

                        
                    if creationTime >= startTime and creationTime <= endTime :
                        timeDelta = retweetTime - creationTime
                        yield tweetid, (int((timeDelta.days * 86400 + timeDelta.seconds)), retweetCount, favoritesCount)
                            
    def reducer(self, tweetid, timestamps):

        rt_rates = [0] * self.NUM_PARTITIONS
        fv_rates = [0] * self.NUM_PARTITIONS
        for t, rc, fc in timestamps :
            bin = t / self.PARTITION_SECS;
            for b in xrange(bin,self.NUM_PARTITIONS):
                fv_rates[b] = max(fv_rates[b], fc)
                rt_rates[b] = max(rt_rates[b], rc)
                
            
        yield None, '%s %s %s ' % (str(tweetid), ' '.join(map(str, rt_rates)),' '.join(map(str, fv_rates)))
            

if __name__ == '__main__':
    MRRetweetRate.run()
