from mrjob.job import MRJob
import json as simplejson
from datetime import datetime, timedelta

class MRRetweetRate(MRJob):
	
	def mapper_get_hour_day(self, _, line):
		startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
		tweet = simplejson.loads(line)
		language = 'es'
		if 'twitter_lang' in tweet : 
			language = tweet['twitter_lang']

		if language == 'en' :
			if 'id' in tweet and 'retweetCount' in tweet :
				tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
				tweeter = tweet['actor']['id'][len('id:twitter.com:') :]
				retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
				creationTime = retweetTime
				if tweet['retweetCount'] > 0 :
					tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]	
					tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
					creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
				if (creationTime - startTime).total_seconds() > 0 :
					if 'utcOffset' in tweet['actor'] and tweet['actor']['utcOffset'] is not None:
						offset = int(tweet['actor']['utcOffset'])/3600
						yield tweetid, (creationTime.hour + offset, creationTime.weekday())
										
				
	def reducer_output_hour_day(self, tweetid, stat):
		dayofweek = 0
		hour = 0
		for hr, day in stat :
			hour = hr
			dayofweek = day
			break

		yield None, ('%s' % ' ' + tweetid + ' ' + str(hour) + ' ' + str(dayofweek))
	
	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_hour_day,
				reducer=self.reducer_output_hour_day)
		]

if __name__ == '__main__':
    MRRetweetRate.run()
