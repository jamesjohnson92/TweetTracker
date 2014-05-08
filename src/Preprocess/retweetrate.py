
from datetime import datetime, timedelta
import simplejson

f = open('tweets.txt', 'r')
startTime = datetime.strptime('2014-03-01T00:00:00.000Z', '%Y-%m-%dT%H:%M:%S.%fZ') 

retweetRates = {}
for line in f :
	tweet = simplejson.loads(line)
	if 'twitter_lang' in tweet : 
		language = tweet['twitter_lang']
		if language == 'en' :
			if 'id' in tweet and 'retweetCount' in tweet :
				tweetid = tweet['id'][len('tag:search.twitter.com,2005:') : ]
				retweetTime = datetime.strptime(tweet['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
				creationTime = retweetTime
				if tweet['retweetCount'] > 0 :
					tweetid = tweet['object']['id'][len('object:search.twitter.com,2005:') :]
					creationTime = datetime.strptime(tweet['object']['postedTime'], '%Y-%m-%dT%H:%M:%S.%fZ')

				if creationTime >= startTime :
					timeDelta = retweetTime - creationTime
					rates = []
					if tweetid in retweetRates :
						rates = retweetRates[tweetid]
					
					rates.append(int((timeDelta.days * 86400 + timeDelta.seconds)/300))
					retweetRates[tweetid] = rates

for k, v in retweetRates.items() :
	print k, ' '.join(map(str, v))