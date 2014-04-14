import simplejson

f = open('tweets.txt', 'r')
f2 = open('stopwords.txt', 'r')
stopwords = [line.strip() for line in f2.readlines()]
f2.close()

USERID_PREFIX = 'id:twitter.com:'

wordCounts = {}
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
		words = body.split()
		for w in words :
			if w not in stopwords and 'http' not in w:
				if w in wordCounts :
					wordCounts[w] = wordCounts[w] + 1
				else :
					wordCounts[w] = 1
	
for k, v in wordCounts.items() :
	print k, v

f.close()
