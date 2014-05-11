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
				tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
				tweeter = tweet['actor']['id'][len('id:twitter.com:') :]
				retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
				creationTime = retweetTime
				if tweet['retweetCount'] > 0 :
					tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
					creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')

				if (creationTime - startTime).total_seconds() > 0 :
					timeDelta = (retweetTime - startTime).total_seconds() - (creationTime - startTime).total_seconds()	
					yield tweetid, (str(tweeter), int(timeDelta/300))
				
	def reducer_compute_rate(self, tweetid, timestamps):
		timeIndexes = []
		users = []
		for (u, t) in timestamps : 
			timeIndexes.append(t)
			users.append(u)

		maxTimeIndex = max(timeIndexes)
		rates = [0 for i in xrange(maxTimeIndex + 1)]
		for t in timeIndexes :
			rates[t] = rates[t] + 1

		start = 0
		breakout = 1
		for r in rates :
			if r < start or r < 5:
				breakout = 0
				break
			start = r

		if breakout == 1 :
			yield tweetid, ' '.join(map(str, rates))
			# the space at the end is important!!!! #blackmagic		

	def reducer_edgeweights(self, user, counts):
		tweets = 0
		retweetCounts = 0
		retweets = {}
		for c in counts :
			if c == "T" :
				tweets = tweets + 1
			else :
				retweetCounts = retweetCounts + 1
				if c in retweets :
					retweets[c] = retweets[c] + 1
				else :
					retweets[c] = 1

		yield user, (str(tweets), str(retweetCounts))
		for k, v in retweets.items() :
			yield user, (k, str(v))

	def steps(self):
		return [
			self.mr(mapper=self.mapper_get_times,
				reducer=self.reducer_compute_rate)
		#	self.mr(reducer=self.reducer_edgeweights)
		]

if __name__ == '__main__':
    MRRetweetRate.run()
