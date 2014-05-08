
import sys
import simplejson

f = open('tweets.txt', 'r')

followers = {}
for line in f :
	tweet = simplejson.loads(line)
	language = 'es'
	retweetCount = 0
	if 'retweetCount' and 'twitter_lang' in tweet :
		language = tweet['twitter_lang']
		retweetCount = tweet['retweetCount']

	if language == 'en' and retweetCount > 0:
		tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]
		retweeter = tweet['actor']['id'][len('id:twitter.com:') :]
		fs = []
		if tweeter in followers :
			fs = followers[tweeter]
		fs.append(retweeter)
		followers[tweeter] = fs

degrees = {}
for k, v in followers.items() :
	deg = len(v)
	if deg in degrees :
		degrees[deg] = degrees[deg] + 1
	else :
		degrees[deg] = 1

for k, v in degrees.items() :
	print str(k) + ', ' + str(v)