
from datetime import datetime, timedelta
import simplejson

f = open('data/tweets1.txt', 'r')
startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ')

retweetRates = {}
for line in f :
	tweet = simplejson.loads(line)
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
					print timeDelta
					rates = []
					if tweetid in retweetRates :
						rates = retweetRates[tweetid]
					
					rates.append((tweeter, int(timeDelta/300)))
					retweetRates[tweetid] = rates

breakoutTweetCounts = {}
breakoutRetweetCounts = {}

for k, v in retweetRates.items() :
	timeIndexes = [t for (u,t) in v]
	users = [u for (u, t) in v]
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
		tweeter = users[0]
		if tweeter in breakoutTweetCounts :
			breakoutTweetCounts[tweeter] = breakoutTweetCounts[tweeter] + 1
		else :
			breakoutTweetCounts[tweeter] = 1

		for i in range(1, len(users)) :
			u = users[i]
			if u in breakoutRetweetCounts :
				breakoutRetweetCounts[u] = breakoutRetweetCounts[u] + 1
			else :
				breakoutRetweetCounts[u] = 1


for k, v in breakoutTweetCounts.items() :
	stat = str(k) + ', ' + str(v)
	if k in breakoutRetweetCounts :
		stat = stat + ', ' + str(breakoutRetweetCounts[k])

	print stat

for k, v in breakoutRetweetCounts.items() :
	if k not in breakoutTweetCounts :
		print str(k) + ', , ' + str(v)