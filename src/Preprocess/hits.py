
import sys
import simplejson

f = open('followers', 'r')
f2 = open('edgeweights', 'r')

graph = {}
hub = {}
authority = {}
retweetweights = {}

numbreakout_retweets = {}
numbreakout_tweets = {}
for l in f :
	data = l.split()
	user = data[0]
	follows = data[1]
	entry = []
	if follows in graph :
		entry = graph[follows]
	entry.append(user)
	graph[follows] = entry
	hub[user] = 1.0
	authority[user] = 1.0
	hub[follows] = 1.0
	authority[follows] = 1.0
	
for u in graph.keys() :
	hub[u] = 1.0
	authority[u] = 1.0

for l in f2 :
	data = l.split()
	user = data[0]
	follower = data[1]
	count = int(data[2])
	entry = {}
	if user in retweetweights :
		entry = retweetweights[user]
	entry[follower] = count
	retweetweights[user] = entry

for i in range(20) :
	for user in hub.keys() :
		sumofhubs = 1.0
		sumofauths = 1.0
		numretweets = 0.0
		if user in retweetweights :
			for friend, count in retweetweights[user].items() :
				numretweets = numretweets + count

			for u in graph[user] :
				retweetwt = 0.0
				if user in retweetweights and u in retweetweights[user] :
					retweetwt = retweetweights[user][u]
			
				sumofhubs = sumofhubs + (1.0 + retweetwt/numretweets) * authority[u]
				sumofauths = sumofauths + (1.0 + retweetwt/numretweets) * hub[u]
		hub[user] = sumofhubs
		authority[user] = sumofauths

for u in hub.keys() :
	print u, round(hub[u], 2)