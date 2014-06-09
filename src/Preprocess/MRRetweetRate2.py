from mrjob.job import MRJob
import json as simplejson
from mrjob.protocol import RawValueProtocol
from datetime import datetime, timedelta
import ujson

class MRRetweetRate(MRJob):
	

    OUTPUT_PROTOCOL = RawValueProtocol


    PARTITION_SECS = 20 # bin things in five minutes
    NUM_PARTITIONS = 60*60/PARTITION_SECS # record for an hour

    def mapper(self, _, line):
        try:
            tweet = ujson.loads(line)
        except:
            tweet = {}
        startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        endTime = datetime.strptime('2014-03-05T23:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
        if 'twitter_lang' in tweet :
            print "hi"

            language = tweet['twitter_lang']
            if language == 'en' :
                if 'id' in tweet and 'retweetCount' in tweet :
                    retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    creationTime = retweetTime
                    favoritesCount = tweet['favoritesCount']
                    retweetCount = tweet['retweetCount']
                    fols = tweet['actor']['followersCount']
                    frds = tweet['actor']['friendsCount']
                    favs = tweet['actor']['favoritesCount']

                    if tweet['retweetCount'] > 0 :
                        tweetid = tweet['object']['id'].split(':')[2]
                        creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                        favoritesCount = tweet['object']['favoritesCount']
                        fols = tweet['object']['actor']['followersCount']
                        frds = tweet['object']['actor']['friendsCount']
                        favs = tweet['object']['actor']['favoritesCount']

                        print "hi again"

                        if creationTime >= startTime and creationTime <= endTime :
                            timeDelta = retweetTime - creationTime
                            yield tweetid, (fols,frds,favs,int((timeDelta.days * 86400 + timeDelta.seconds)), retweetCount, favoritesCount)
                            
    def reducer(self, tweetid, timestamps):

        rt_rates = [0] * self.NUM_PARTITIONS
        fv_rates = [0] * self.NUM_PARTITIONS
        num_fols = None
        num_frds = 0
        num_favs = 0
        for fols,frds,favs,t, rc, fc in timestamps :
            if num_fols == None:
                num_fols = fols
                num_frds = frds
                num_favs = favs
            else :
                num_fols = min(num_fols,fols)
                num_frds = min(num_frds,frds)
                num_favs = min(num_favs,favs)
            bin = t / self.PARTITION_SECS;
            for b in xrange(bin,self.NUM_PARTITIONS):
                fv_rates[b] = max(fv_rates[b], fc)
                rt_rates[b] = max(rt_rates[b], rc)
                
            
        yield None, '%s %s %s %s %s %s ' % (str(tweetid), str(num_fols), str(num_frds), str(num_favs),
                                            ' '.join(map(str, rt_rates)),' '.join(map(str, fv_rates)))
            

if __name__ == '__main__':
    MRRetweetRate.run()
