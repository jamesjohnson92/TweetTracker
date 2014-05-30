from mrjob.job import MRJob
import json as simplejson
from datetime import datetime, timedelta

class MRRetweetRate(MRJob):
	
	def mapper_get_times(self, _, line):
		startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
		tweet = simplejson.loads(line)
		language = 'es'
		if 'twitter_lang' in tweet : 
			language = tweet['twitter_lang']

		if language == 'en' :
			if 'id' in tweet and 'retweetCount' in tweet :
				retweeter = tweet['actor']['id'][len('id:twitter.com:') :]
				if tweet['retweetCount'] > 0 :
					tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]	
					yield  tweeter, retweeter
				
	def reducer_compute_rate(self, user, followers):
		retweeters = {}
		for u in followers :
			if u in retweeters :
				retweeters[u] = retweeters[u] + 1
			else :
				retweeters[u] = 1

		for k, v in retweeters.items() :
			yield "", ('%s' % user + ' ' + k + ' ' + str(v))
	
	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_times,
				reducer=self.reducer_compute_rate)
		]

if __name__ == '__main__':
    MRRetweetRate.run()
