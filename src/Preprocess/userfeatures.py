from mrjob.job import MRJob
import json as simplejson
from datetime import datetime, timedelta

class MRRetweetRate(MRJob):
	
	def mapper_get_user_features(self, _, line):
		startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
		tweet = simplejson.loads(line)
		language = 'es'
		if 'twitter_lang' in tweet : 
			language = tweet['twitter_lang']

		if language == 'en' :
			if 'id' in tweet and 'retweetCount' in tweet :
				tweeter = tweet['actor']['id'][len('id:twitter.com:') :]

				if tweet['retweetCount'] > 0 :
					tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]	
				
	def reducer_output_user_features(self, tweetid, stat):
		dayofweek = 0
		hour = 0
		for hr, day in stat :
			hour = hr
			dayofweek = day
			break

		yield tweetid, (hour, dayofweek)
	
	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_hour_day,
				reducer=self.reducer_output_hour_day)
		]

if __name__ == '__main__':
    MRRetweetRate.run()
