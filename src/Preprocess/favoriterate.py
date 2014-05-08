
from datetime import datetime, timedelta
import simplejson

f = open('tweets.txt', 'r')
startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 

favoriteCounts = {}

for line in f :
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
						counts = []
						if tweetid in favoriteCounts :
							counts = favoriteCounts[tweetid]
				
						counts.append((int((timeDelta.days * 86400 + timeDelta.seconds)/300), favoritesCount))
						favoriteCounts[tweetid] = counts

for k, v in favoriteCounts.items() :
	print k, ' '.join(map(str, v))