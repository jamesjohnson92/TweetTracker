from mrjob.job import MRJob
import json as simplejson
from datetime import datetime, timedelta

class MRRetweetRate(MRJob):
	

    PARTITION_SECS = 60*5 # bin things in five minutes
    NUM_PARTITIONS = 60*60/PARTITION_SECS # record for an hour

    def mapper(self, _, line):
        startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        endTime = datetime.strptime('2014-03-01T20:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        tweet = simplejson.loads(line)
        if 'twitter_lang' in tweet : 
            language = tweet['twitter_lang']
            if language == 'en' :
                if 'id' in tweet and 'retweetCount' in tweet :
                    tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
                    retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    creationTime = retweetTime
                    favoritesCount = tweet['favoritesCount']
                    if tweet['retweetCount'] > 0 :
                        tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
                        creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        favoritesCount = tweet['object']['favoritesCount']

                        
                    if creationTime >= startTime and creationTime <= endTime :
                        timeDelta = retweetTime - creationTime
                        yield tweetid, (int((timeDelta.days * 86400 + timeDelta.seconds)), favoritesCount)
                            
    def reducer(self, tweetid, timestamps):
        rt_rates = [0] * self.NUM_PARTITIONS
        fv_rates = [0] * self.NUM_PARTITIONS
        for t, fc in timestamps :
            bin = t / self.PARTITION_SECS;
            if bin < len(rt_rates):
                rt_rates[bin] = rt_rates[bin] + 1
                for b in xrange(bin,len(rates)):
                    fv_rates[b] = max(fv_rates[b], fc)
            
        yield None, '%s %s ' % (' '.join(map(lambda x : str(x[0]), rates)),' '.join(map(lambda x : str(x[1]), rates)))
            

if __name__ == '__main__':
    MRRetweetRate.run()
