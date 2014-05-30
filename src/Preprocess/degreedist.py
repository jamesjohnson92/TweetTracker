
import sys
import simplejson

f = open('followers', 'r')

followers = {}
for line in f :
	users = line.split()
	follower = users[0]
	friend = users[1]
	if friend in followers :
		followers[friend] = followers[friend] + 1
	else :
		followers[friend] = 1

degreecounts = {}
for k, v in followers.items() :
	if v in degreecounts :
		degreecounts[v] = degreecounts[v] + 1
	else :
		degreecounts[v] = 1

for k, v in degreecounts.items():
	print str(k) + ',' + str(v)