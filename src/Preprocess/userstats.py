
import sys
import simplejson

f = open('tweets.txt', 'r')

followerCounts = {}
favoriteCounts = {}
statusCounts = {}
friendCounts = {}
atmentionCounts = {}

for line in f :
	tweet = simplejson.loads(line)
	retweetCount = 0
	language = 'es'
	if 'retweetCount' and 'twitter_lang' in tweet :
		language = tweet['twitter_lang']
		retweetCount = tweet['retweetCount']

		

	if language == 'en' :
		for mention in tweet['twitter_entities']['user_mentions'] :
			mid = mention['id']
			if mid in atmentionCounts :
				atmentionCounts[mid] = atmentionCounts[mid] + 1
			else :
				atmentionCounts[mid] = 1

		tweeter = tweet['actor']['id'][len('id:twitter.com:') :]
		followerCounts[tweeter] = tweet['actor']['followersCount']
		statusCounts[tweeter] = tweet['actor']['statusesCount']
		friendCounts[tweeter] = tweet['actor']['friendsCount']
		favoriteCounts[tweeter] = tweet['actor']['favoritesCount']


		if retweetCount > 0 :
			tweeter = tweet['object']['actor']['id'][len('id:twitter.com:') :]
			followerCounts[tweeter] = tweet['object']['actor']['followersCount']
			statusCounts[tweeter] = tweet['object']['actor']['statusesCount']
			friendCounts[tweeter] = tweet['object']['actor']['friendsCount']
			favoriteCounts[tweeter] = tweet['object']['actor']['favoritesCount']


for k, v in followerCounts.items() :
	stat = str(k) + ', folCount, ' + str(v)
	if k in friendCounts :
		stat = stat + ', frenCount, ' + str(friendCounts[k])
	if k in statusCounts :
		stat = stat + ', stCount, ' + str(statusCounts[k])
	if k in favoriteCounts :
		stat = stat + ', favCount, ' + str(favoriteCounts[k])
	if k in atmentionCounts :
		stat = stat + ', @menCount, ' + str(atmentionCounts[k])

	print stat
