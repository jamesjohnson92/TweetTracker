
import simplejson

f = open('tweets.txt', 'r')

USERID_PREFIX = 'id:twitter.com:'
for line in f :
	if len(line) == 0 :
		continue

	tweet = simplejson.loads(line)
	retweetCount = 0
	language = 'es'
	if 'retweetCount' in tweet :
		retweetCount = tweet['retweetCount']
	if 'twitter_lang' in tweet :
		language = tweet['twitter_lang']

	if language == 'en' and retweetCount > 0:
		body = tweet['object']['body']
		tweeter = tweet['object']['actor']['id'][len(USERID_PREFIX) :]
		user = tweet['actor']['id'][len(USERID_PREFIX) :]
		print user, tweeter

f.close()
