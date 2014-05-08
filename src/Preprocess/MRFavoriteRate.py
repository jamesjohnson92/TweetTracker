from mrjob.job import MRJob
import json as simplejson
from datetime import datetime, timedelta

class MRFavoritesRate(MRJob):
	
	def mapper_get_times(self, _, line):
		startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 
		tweet = simplejson.loads(line)
		if 'twitter_lang' in tweet : 
			language = tweet['twitter_lang']
			if language == 'en' :
				if 'id' in tweet and 'favoritesCount' in tweet :
					tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
					retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
					creationTime = retweetTime
					favoritesCount = tweet['favoritesCount']
					if tweet['retweetCount'] > 0 :
						tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
						creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
						favoritesCount = tweet['object']['favoritesCount']

					if creationTime >= startTime :
						timeDelta = retweetTime - creationTime
						yield tweetid, (int((timeDelta.days * 86400 + timeDelta.seconds)/300), favoritesCount)
				
	def reducer_compute_rate(self, tweetid, favoritesCount):
		timeIndexes = []
		favStart = -1
		for t, c in favoritesCount :
			if int(c) > favStart :
				timeIndexes.append(int(t))
				favStart = int(c)

		maxTimeIndex = max(timeIndexes)
		rates = [0 for i in xrange(maxTimeIndex + 1)]
		for t in timeIndexes :
			rates[t] = rates[t] + 1

		yield str(tweetid), '%s ' % ' '.join(map(str, rates))
		# the space at the end is important!!!! #blackmagic		

	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_times,
				reducer=self.reducer_compute_rate)
		]

if __name__ == '__main__':
    MRFavoritesRate.run()
